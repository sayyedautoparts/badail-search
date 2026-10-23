#Requires -Version 5.1
# One-time setup on the PC that has the USB Rongta.
$ErrorActionPreference = "Stop"
$Src = $PSScriptRoot
if (-not $Src) { $Src = Split-Path -Parent $MyInvocation.MyCommand.Path }
$Dest = Join-Path $env:LOCALAPPDATA "BadailPrint\agent"
$TaskName = "BadailRongtaPrintAgent"
try {
  Get-ChildItem -LiteralPath $Src -File -ErrorAction SilentlyContinue | Unblock-File -ErrorAction SilentlyContinue
} catch {}

$Needed = @(
  "rongta_local_print.py",
  "print_rongta_image.ps1",
  "start_rongta_print_agent.ps1"
)
foreach ($f in $Needed) {
  if (-not (Test-Path -LiteralPath (Join-Path $Src $f))) {
    Write-Host "ERROR: Missing $f" -ForegroundColor Red
    Write-Host "Unzip RongtaPrintAgent.zip fully, open the folder, then double-click install.bat"
    exit 1
  }
}

$prn = Get-CimInstance Win32_Printer | Where-Object { $_.Name -like "*Rongta*" } | Select-Object -First 1
if (-not $prn) {
  Write-Host "ERROR: Windows does not have a Rongta printer on this PC." -ForegroundColor Red
  Write-Host "Plug the printer in USB, install the Rongta driver, then run install.bat again."
  Get-CimInstance Win32_Printer | ForEach-Object { Write-Host ("  printer: " + $_.Name) }
  exit 1
}
$PrinterName = [string]$prn.Name
Write-Host "Using printer: $PrinterName"

function Get-BadailPython {
  $hits = @()
  foreach ($p in @(
      (Join-Path $env:LOCALAPPDATA "Python\pythoncore-3.14-64\python.exe"),
      (Join-Path $env:LOCALAPPDATA "Python\pythoncore-3.13-64\python.exe"),
      (Join-Path $env:LOCALAPPDATA "Python\pythoncore-3.12-64\python.exe"),
      (Join-Path $env:LOCALAPPDATA "Programs\Python\Python314\python.exe"),
      (Join-Path $env:LOCALAPPDATA "Programs\Python\Python313\python.exe"),
      (Join-Path $env:LOCALAPPDATA "Programs\Python\Python312\python.exe"),
      (Join-Path $env:LOCALAPPDATA "Python\bin\python.exe")
    )) {
    if (Test-Path -LiteralPath $p) { $hits += $p }
  }
  $c = Get-Command python -ErrorAction SilentlyContinue
  if ($c -and $c.Source -and ($c.Source -notmatch "WindowsApps")) { $hits += $c.Source }
  $py = Get-Command py -ErrorAction SilentlyContinue
  if ($py) { $hits += $py.Source }
  return $hits | Select-Object -First 1
}

$python = Get-BadailPython
if (-not $python) {
  Write-Host "ERROR: Python is not installed on this PC." -ForegroundColor Red
  Write-Host "Install Python from https://www.python.org/downloads/"
  Write-Host "During setup, check: Add python.exe to PATH"
  Write-Host "Then run install.bat again."
  exit 1
}
Write-Host "Using Python: $python"

if (-not (Test-Path -LiteralPath $Dest)) {
  New-Item -ItemType Directory -Path $Dest -Force | Out-Null
}
foreach ($f in $Needed) {
  Copy-Item -LiteralPath (Join-Path $Src $f) -Destination (Join-Path $Dest $f) -Force
}
Copy-Item -LiteralPath (Join-Path $Src "install_rongta_print_agent.ps1") -Destination (Join-Path $Dest "install_rongta_print_agent.ps1") -Force -ErrorAction SilentlyContinue
$Runner = Join-Path $Dest "start_rongta_print_agent.ps1"
$Py = Join-Path $Dest "rongta_local_print.py"

Get-CimInstance Win32_Process | Where-Object {
  $_.CommandLine -and (
    $_.CommandLine -match 'rongta_local_print\.py' -or
    $_.CommandLine -match 'start_rongta_print_agent\.ps1'
  )
} | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

try {
  $action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$Runner`"" `
    -WorkingDirectory $Dest
  $trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
  $settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Days 3650)
  $principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
  try { Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue } catch {}
  Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null
  Write-Host "Scheduled task created: $TaskName"
} catch {
  Write-Host "Note: could not create auto-start task: $($_.Exception.Message)" -ForegroundColor Yellow
}

$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = $python
if ($python -match '\\py\.exe$') { $psi.Arguments = '-3 -u "' + $Py + '"' }
else { $psi.Arguments = '-u "' + $Py + '"' }
$psi.WorkingDirectory = $Dest
$psi.WindowStyle = [System.Diagnostics.ProcessWindowStyle]::Hidden
$psi.CreateNoWindow = $true
$psi.UseShellExecute = $false
$psi.RedirectStandardError = $true
$psi.RedirectStandardOutput = $true
$proc = [System.Diagnostics.Process]::Start($psi)

$ok = $false
foreach ($i in 1..20) {
  Start-Sleep -Milliseconds 400
  try {
    $r = Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:17891/ping" -TimeoutSec 2
    if ($r.StatusCode -eq 200) { $ok = $true; break }
  } catch {}
}
if (-not $ok) {
  $err = ""
  try { $err = $proc.StandardError.ReadToEnd() } catch {}
  Write-Host "ERROR: Print agent did not start on http://127.0.0.1:17891" -ForegroundColor Red
  if ($err) { Write-Host $err }
  Write-Host "Log: $env:LOCALAPPDATA\BadailPrint\agent.log"
  exit 1
}
Write-Host ""
Write-Host "OK. Print agent is running on this PC." -ForegroundColor Green
Write-Host "Printer: $PrinterName"
Write-Host "Open the website, print from THIS computer, and pick this Rongta in the print window."
exit 0
