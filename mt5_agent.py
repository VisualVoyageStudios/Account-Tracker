import json
import time
import urllib.request

import MetaTrader5 as mt5

SERVER = "https://account-tracker-pjon.onrender.com"   # your Render URL, no trailing slash
PUSH_KEY = "2.KBWWAopm8sfNEnbxf6hO0Hxqgi_XXmkt"                                  # the key shown when you added the account
TERMINAL_PATH = ""                             # optional: full path to terminal64.exe
POLL_SECONDS = 4

def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        SERVER + path, data=data, method=method,
        headers={"Content-Type": "application/json", "X-Push-Key": PUSH_KEY},
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def connect():
    ok = mt5.initialize(path=TERMINAL_PATH) if TERMINAL_PATH else mt5.initialize()
    if not ok:
        raise RuntimeError(f"Can't reach MT5 {mt5.last_error()}. Is the terminal open and logged in?")


def snapshot():
    info = mt5.account_info()
    if info is None:
        raise RuntimeError(f"MT5 returned no account info {mt5.last_error()}")
    return {
        "balance": info.balance,
        "equity": info.equity,
        "positions": mt5.positions_total() or 0,
        "currency": info.currency,
    }


def main():
    if not SERVER or not PUSH_KEY:
        raise SystemExit("SERVER and PUSH_KEY are not set. Edit them in sync_agent.bat.")
    print("Sync agent running. Click Sync in the dashboard. Close this window to stop.")
    connected = False
    while True:
        try:
            if call("GET", "/agent/poll").get("sync"):
                if not connected:
                    connect()
                    connected = True
                call("POST", "/push", snapshot())
                print(time.strftime("%H:%M:%S"), "synced")
        except Exception as e:
            connected = False          # reconnect to MT5 on the next request
            print(time.strftime("%H:%M:%S"), "error:", e)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()