@echo off
setlocal
cd /d "%~dp0"

powershell -NoProfile -Command "if (Test-NetConnection 127.0.0.1 -Port 8787 -InformationLevel Quiet) { Start-Process 'http://127.0.0.1:8787'; exit 0 } else { exit 1 }" >nul 2>&1
if %errorlevel% equ 0 exit /b 0

where node >nul 2>nul
if errorlevel 1 (
  echo Node.js was not found. Install Node.js 20 or newer, then double-click this file again.
  pause
  exit /b 1
)

if not exist "node_modules\megajs\package.json" (
  echo Installing required packages...
  call npm install
  if errorlevel 1 goto failed
)

start "" /b cmd /c "timeout /t 2 /nobreak >nul & start http://127.0.0.1:8787"
call npm start

:failed
echo.
echo The downloader stopped. Check the error above.
pause
