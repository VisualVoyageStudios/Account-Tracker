import os, json, time, hmac, hashlib, asyncio, secrets
from datetime import datetime, timezone

import httpx
import websockets
from cryptography.fernet import Fernet
from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text
from sqlalchemy.orm import sessionmaker, declarative_base, Session

DASH_TOKEN = os.getenv("DASH_TOKEN", "")
FERNET_KEY = os.getenv("FERNET_KEY", "")
DERIV_APP_ID = os.getenv("DERIV_APP_ID", "")
ALLOWED_ORIGINS = [o.strip().rstrip("/") for o in os.getenv(
    "ALLOWED_ORIGIN",
    "https://visualvoyagestudios.github.io,http://localhost:5500,http://127.0.0.1:5500",
).split(",") if o.strip()]

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./tracker.db").strip()
# Force the psycopg2 driver we install (SQLAlchemy 2.1 would default to psycopg 3)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = "postgresql+psycopg2://" + DATABASE_URL[len("postgres://"):]
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = "postgresql+psycopg2://" + DATABASE_URL[len("postgresql://"):]

if len(DASH_TOKEN) < 24 or not FERNET_KEY:
    raise RuntimeError("Set DASH_TOKEN (24+ chars) and FERNET_KEY")
fernet = Fernet(FERNET_KEY.encode())

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {})
SessionLocal = sessionmaker(bind=engine, autoflush=False)
Base = declarative_base()


class Account(Base):
    __tablename__ = "accounts"
    id = Column(Integer, primary_key=True)
    broker = Column(String, nullable=False)       # display name: Exness, Deriv...
    label = Column(String, nullable=False)        # e.g. "Exness Standard 1234"
    kind = Column(String, nullable=False)         # deriv | bybit | manual
    currency = Column(String, default="USD")
    usd_rate = Column(Float, default=1.0)         # 1 unit of currency in USD
    creds_enc = Column(Text, nullable=True)
    balance = Column(Float, default=0.0)
    equity = Column(Float, default=0.0)
    open_positions = Column(Integer, default=0)
    error = Column(String, nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=True)


class TotalSnap(Base):
    __tablename__ = "total_snaps"
    id = Column(Integer, primary_key=True)
    ts = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    total_usd = Column(Float, nullable=False)


Base.metadata.create_all(engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def require_auth(authorization: str = Header(default="")):
    token = authorization.removeprefix("Bearer ").strip()
    if not hmac.compare_digest(token, DASH_TOKEN):
        raise HTTPException(401, "Unauthorized")


# ---------------- Connectors ----------------
async def _deriv_call(ws, payload, rid):
    await ws.send(json.dumps({**payload, "req_id": rid}))
    while True:
        msg = json.loads(await asyncio.wait_for(ws.recv(), 15))
        if msg.get("req_id") == rid:
            if "error" in msg:
                raise RuntimeError(msg["error"].get("message", "Deriv error"))
            return msg


DERIV_API = "https://api.derivws.com"

async def fetch_deriv(creds: dict) -> dict:
    app_id = str(creds.get("app_id") or DERIV_APP_ID)
    headers = {"Authorization": f"Bearer {creds['token']}", "Deriv-App-ID": app_id}
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.get(f"{DERIV_API}/trading/v1/options/accounts", headers=headers)
        if r.status_code != 200:
            raise RuntimeError(f"Deriv {r.status_code}: {r.text[:120]}")
        body = r.json()
        items = body.get("data", body) if isinstance(body, dict) else body
        if isinstance(items, dict):
            items = items.get("accounts", [items])
        if not items:
            raise RuntimeError("Deriv returned no accounts")

        wanted = (creds.get("account_id") or "").strip()
        if wanted:
            acct = next((x for x in items if x.get("account_id") == wanted), None)
            if not acct:
                raise RuntimeError("Account ID not found. Yours: " + ", ".join(str(x.get("account_id")) for x in items))
        elif len(items) == 1:
            acct = items[0]
        else:
            raise RuntimeError("Several accounts found. Add one as the account ID: " + ", ".join(
                f"{x.get('account_id')} ({x.get('group', '')})" for x in items))

        bal = float(acct.get("balance") or 0)

        # Open positions: best effort, never fails the sync
        n = 0
        try:
            o = await c.post(f"{DERIV_API}/trading/v1/options/accounts/{acct['account_id']}/otp", headers=headers)
            url = o.json()["data"]["url"]
            async with websockets.connect(url, open_timeout=15) as ws:
                pf = await _deriv_call(ws, {"portfolio": 1}, 1)
                n = len(pf.get("portfolio", {}).get("contracts", []))
        except Exception:
            n = 0
        return {"balance": bal, "equity": bal, "open_positions": n, "currency": acct.get("currency", "USD")}


async def fetch_bybit(creds: dict) -> dict:
    api_key, secret = creds["api_key"], creds["api_secret"]
    recv = "5000"
    ts = str(int(time.time() * 1000))
    query = "accountType=UNIFIED"
    sig = hmac.new(secret.encode(), (ts + api_key + recv + query).encode(), hashlib.sha256).hexdigest()
    headers = {"X-BAPI-API-KEY": api_key, "X-BAPI-TIMESTAMP": ts, "X-BAPI-RECV-WINDOW": recv, "X-BAPI-SIGN": sig}
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get(f"https://api.bybit.com/v5/account/wallet-balance?{query}", headers=headers)
        data = r.json()
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit: {data.get('retMsg', 'error')}")
        acct = data["result"]["list"][0]
        equity = float(acct.get("totalEquity") or 0)
        wallet = float(acct.get("totalWalletBalance") or equity)
        pos = await c.get(
            "https://api.bybit.com/v5/position/list?category=linear&settleCoin=USDT",
            headers={**headers, "X-BAPI-SIGN": hmac.new(secret.encode(), (ts + api_key + recv + "category=linear&settleCoin=USDT").encode(), hashlib.sha256).hexdigest()},
        )
        n = 0
        pj = pos.json()
        if pj.get("retCode") == 0:
            n = sum(1 for p in pj["result"]["list"] if float(p.get("size") or 0) > 0)
        return {"balance": wallet, "equity": equity, "open_positions": n, "currency": "USD"}


CONNECTORS = {"deriv": fetch_deriv, "bybit": fetch_bybit}


# ---------------- API ----------------
class AccountIn(BaseModel):
    broker: str
    label: str
    kind: str                      # deriv | bybit | manual
    currency: str = "USD"
    usd_rate: float = 1.0
    credentials: dict = {}
    balance: float = 0.0


class AccountPatch(BaseModel):
    label: str | None = None
    balance: float | None = None
    equity: float | None = None
    open_positions: int | None = None
    usd_rate: float | None = None
    currency: str | None = None


app = FastAPI(title="Account Tracker")
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS, allow_methods=["*"], allow_headers=["*"])


def out(a: Account) -> dict:
    return {
        "id": a.id, "broker": a.broker, "label": a.label, "kind": a.kind, "currency": a.currency,
        "usd_rate": a.usd_rate, "balance": a.balance, "equity": a.equity,
        "equity_usd": round((a.equity or 0) * (a.usd_rate or 1), 2),
        "open_positions": a.open_positions, "error": a.error,
        "updated_at": a.updated_at.isoformat() if a.updated_at else None,
    }


@app.get("/overview", dependencies=[Depends(require_auth)])
def overview(db: Session = Depends(get_db)):
    accts = db.query(Account).order_by(Account.broker, Account.id).all()
    snaps = db.query(TotalSnap).order_by(TotalSnap.ts.desc()).limit(300).all()
    snaps.reverse()
    return {
        "accounts": [out(a) for a in accts],
        "total_usd": round(sum((a.equity or 0) * (a.usd_rate or 1) for a in accts), 2),
        "history": [{"ts": s.ts.isoformat(), "total_usd": s.total_usd} for s in snaps],
    }


@app.post("/accounts", dependencies=[Depends(require_auth)])
def add_account(p: AccountIn, db: Session = Depends(get_db)):
    if p.kind not in ("deriv", "bybit", "manual", "mt"):
        raise HTTPException(400, "kind must be deriv, bybit, manual or mt")
    if p.kind == "deriv" and not p.credentials.get("token"):
        raise HTTPException(400, "Deriv needs a read-only API token")
    if p.kind == "bybit" and not (p.credentials.get("api_key") and p.credentials.get("api_secret")):
        raise HTTPException(400, "Bybit needs api_key and api_secret (read-only)")

    creds = dict(p.credentials)
    secret = None
    if p.kind == "mt":
        secret = secrets.token_urlsafe(24)
        creds = {"push_hash": hashlib.sha256(secret.encode()).hexdigest()}

    a = Account(
        broker=p.broker[:50], label=p.label[:100], kind=p.kind, currency=p.currency.upper()[:8],
        usd_rate=p.usd_rate, balance=p.balance, equity=p.balance,
        creds_enc=fernet.encrypt(json.dumps(creds).encode()).decode() if creds else None,
        updated_at=datetime.now(timezone.utc),
    )
    db.add(a)
    db.commit()
    db.refresh(a)
    result = out(a)
    if secret:
        result["push_key"] = f"{a.id}.{secret}"
    return result


@app.patch("/accounts/{aid}", dependencies=[Depends(require_auth)])
def patch_account(aid: int, p: AccountPatch, db: Session = Depends(get_db)):
    a = db.get(Account, aid)
    if not a:
        raise HTTPException(404, "Not found")
    for k, v in p.model_dump(exclude_none=True).items():
        setattr(a, k, v)
    if p.balance is not None and p.equity is None and a.kind == "manual":
        a.equity = p.balance
    a.updated_at = datetime.now(timezone.utc)
    db.commit()
    return out(a)


@app.delete("/accounts/{aid}", dependencies=[Depends(require_auth)])
def delete_account(aid: int, db: Session = Depends(get_db)):
    a = db.get(Account, aid)
    if not a:
        raise HTTPException(404, "Not found")
    db.delete(a)
    db.commit()
    return {"ok": True}


async def _refresh_one(a: Account):
    try:
        creds = json.loads(fernet.decrypt(a.creds_enc.encode())) if a.creds_enc else {}
        d = await CONNECTORS[a.kind](creds)
        a.balance, a.equity, a.open_positions = d["balance"], d["equity"], d["open_positions"]
        if a.kind == "bybit":
            a.currency, a.usd_rate = "USD", 1.0
        elif d.get("currency"):
            a.currency = d["currency"]
        a.error = None
        a.updated_at = datetime.now(timezone.utc)
    except Exception as e:
        a.error = str(e)[:200]


@app.post("/refresh", dependencies=[Depends(require_auth)])
async def refresh(db: Session = Depends(get_db)):
    live = db.query(Account).filter(Account.kind.in_(CONNECTORS.keys())).all()
    await asyncio.gather(*[_refresh_one(a) for a in live])
    db.commit()
    accts = db.query(Account).all()
    total = round(sum((a.equity or 0) * (a.usd_rate or 1) for a in accts), 2)
    db.add(TotalSnap(total_usd=total))
    db.commit()
    return {"total_usd": total}


# ---------------- MT5 sync agent ----------------
SYNC_REQUESTS: dict[int, float] = {}   # account id -> time the Sync button was clicked


class PushIn(BaseModel):
    balance: float
    equity: float
    positions: int = 0
    currency: str = "USD"


def _auth_push(key: str, db: Session) -> Account:
    bad = HTTPException(401, "Invalid push key")
    try:
        aid_s, secret = key.split(".", 1)
        aid = int(aid_s)
    except ValueError:
        raise bad
    a = db.get(Account, aid)
    if not a or a.kind != "mt" or not a.creds_enc:
        raise bad
    stored = json.loads(fernet.decrypt(a.creds_enc.encode())).get("push_hash", "")
    if not hmac.compare_digest(hashlib.sha256(secret.encode()).hexdigest(), stored):
        raise bad
    return a


@app.post("/sync-request", dependencies=[Depends(require_auth)])
def sync_request(db: Session = Depends(get_db)):
    ids = [a.id for a in db.query(Account).filter(Account.kind == "mt").all()]
    now = time.time()
    for i in ids:
        SYNC_REQUESTS[i] = now
    return {"requested": ids}


@app.get("/agent/poll")
def agent_poll(x_push_key: str = Header(default=""), db: Session = Depends(get_db)):
    a = _auth_push(x_push_key, db)
    t = SYNC_REQUESTS.get(a.id)
    return {"sync": bool(t and time.time() - t < 120)}


@app.post("/push")
def push(p: PushIn, x_push_key: str = Header(default=""), db: Session = Depends(get_db)):
    a = _auth_push(x_push_key, db)
    SYNC_REQUESTS.pop(a.id, None)

    a.balance, a.equity, a.open_positions = p.balance, p.equity, max(0, p.positions)
    a.currency = p.currency.upper()[:8]
    if a.currency == "USD":
        a.usd_rate = 1.0          # other currencies keep the rate you set
    a.error = None
    now = datetime.now(timezone.utc)
    a.updated_at = now
    db.commit()

    # Add a history point for the chart, at most once a minute
    last = db.query(TotalSnap).order_by(TotalSnap.ts.desc()).first()
    last_ts = None
    if last:
        last_ts = last.ts if last.ts.tzinfo else last.ts.replace(tzinfo=timezone.utc)
    if not last_ts or (now - last_ts).total_seconds() > 60:
        total = sum((x.equity or 0) * (x.usd_rate or 1) for x in db.query(Account).all())
        db.add(TotalSnap(total_usd=round(total, 2)))
        db.commit()
    return {"ok": True}


# ---------------- MT5 sync agent ----------------
SYNC_REQUESTS: dict[int, float] = {}   # account id -> time the Sync button was clicked


class PushIn(BaseModel):
    balance: float
    equity: float
    positions: int = 0
    currency: str = "USD"


def _auth_push(key: str, db: Session) -> Account:
    bad = HTTPException(401, "Invalid push key")
    try:
        aid_s, secret = key.split(".", 1)
        aid = int(aid_s)
    except ValueError:
        raise bad
    a = db.get(Account, aid)
    if not a or a.kind != "mt" or not a.creds_enc:
        raise bad
    stored = json.loads(fernet.decrypt(a.creds_enc.encode())).get("push_hash", "")
    if not hmac.compare_digest(hashlib.sha256(secret.encode()).hexdigest(), stored):
        raise bad
    return a


@app.post("/sync-request", dependencies=[Depends(require_auth)])
def sync_request(db: Session = Depends(get_db)):
    ids = [a.id for a in db.query(Account).filter(Account.kind == "mt").all()]
    now = time.time()
    for i in ids:
        SYNC_REQUESTS[i] = now
    return {"requested": ids}


@app.get("/agent/poll")
def agent_poll(x_push_key: str = Header(default=""), db: Session = Depends(get_db)):
    a = _auth_push(x_push_key, db)
    t = SYNC_REQUESTS.get(a.id)
    return {"sync": bool(t and time.time() - t < 120)}


@app.post("/push")
def push(p: PushIn, x_push_key: str = Header(default=""), db: Session = Depends(get_db)):
    a = _auth_push(x_push_key, db)
    SYNC_REQUESTS.pop(a.id, None)

    a.balance, a.equity, a.open_positions = p.balance, p.equity, max(0, p.positions)
    a.currency = p.currency.upper()[:8]
    if a.currency == "USD":
        a.usd_rate = 1.0          # other currencies keep the rate you set
    a.error = None
    now = datetime.now(timezone.utc)
    a.updated_at = now
    db.commit()

    # Add a history point for the chart, at most once a minute
    last = db.query(TotalSnap).order_by(TotalSnap.ts.desc()).first()
    last_ts = None
    if last:
        last_ts = last.ts if last.ts.tzinfo else last.ts.replace(tzinfo=timezone.utc)
    if not last_ts or (now - last_ts).total_seconds() > 60:
        total = sum((x.equity or 0) * (x.usd_rate or 1) for x in db.query(Account).all())
        db.add(TotalSnap(total_usd=round(total, 2)))
        db.commit()
    return {"ok": True}