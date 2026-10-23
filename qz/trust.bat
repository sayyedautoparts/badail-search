@echo off
echo Trusting this website in QZ Tray...
set "DEST=%TEMP%\badail-qz-trust"
if not exist "%DEST%" mkdir "%DEST%"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "Invoke-WebRequest -UseBasicParsing -Uri 'https://badail-search.vercel.app/qz/trust-qz.ps1' -OutFile (Join-Path $env:TEMP 'badail-qz-trust\trust-qz.ps1')"
if errorlevel 1 (
  echo Failed to download trust-qz.ps1
  pause
  exit /b 1
)
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%DEST%\trust-qz.ps1"
echo.
pause
