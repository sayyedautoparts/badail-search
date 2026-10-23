@echo off
cd /d "%~dp0"
echo Installing Rongta print agent on this PC...
echo.
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0install_rongta_print_agent.ps1"
echo.
pause
