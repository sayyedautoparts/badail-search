@echo off
echo Trusting this website in QZ Tray (one time)...
if not exist "%PROGRAMDATA%\qz" mkdir "%PROGRAMDATA%\qz"
powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "New-Item -ItemType Directory -Force -Path $env:PROGRAMDATA\qz | Out-Null; Invoke-WebRequest -UseBasicParsing -Uri 'https://badail-search.vercel.app/qz/cert' -OutFile (Join-Path $env:PROGRAMDATA 'qz\override.crt')"
if errorlevel 1 (
  echo Failed to download the certificate.
  pause
  exit /b 1
)
echo.
echo OK. Close QZ Tray completely, open it again, then print.
echo After that, Allow should not appear every time.
pause
