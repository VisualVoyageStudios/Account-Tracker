@echo off
title MT5 Sync Agent
cd /d "%~dp0"

rem ---- EDIT THESE THREE LINES ----
set "SERVER=https://account-tracker-pjon.onrender.com"
set "PUSH_KEY=2.KBWWAopm8sfNEnbxf6hO0Hxqgi_XXmkt"
set "TERMINAL_PATH="
rem TERMINAL_PATH is optional, e.g. C:\Program Files\MetaTrader 5 EXNESS\terminal64.exe
rem ---------------------------------

if not exist "%~dp0mt5_agent.py" (
  echo mt5_agent.py was not found in:
  echo   %~dp0
  echo Keep sync_agent.bat and mt5_agent.py in the same folder.
  pause
  exit /b 1
)

where python >nul 2>nul
if errorlevel 1 (
  echo Python was not found. Install it from python.org and tick "Add Python to PATH".
  pause
  exit /b 1
)

python -c "import MetaTrader5" >nul 2>nul
if errorlevel 1 (
  echo Installing the MetaTrader5 package...
  python -m pip install MetaTrader5
)

:loop
python "%~dp0mt5_agent.py"
echo.
echo Agent stopped. Restarting in 5 seconds. Close this window to quit.
timeout /t 5 >nul
goto loop