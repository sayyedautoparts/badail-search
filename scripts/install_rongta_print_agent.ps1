#Requires -Version 5.1
# One-time setup on the PC that has the USB Rongta.
$ErrorActionPreference = "Stop"
$Src = $PSScriptRoot
$Dest = Join-Path $env:LOCALAPPDATA "BadailPrint\agent"
$TaskName = "BadailRongtaPrintAgent"
$Needed = @(
  "rongta_local_print.py",
  "print_rongta_image.ps1",
  "start_rongta_print_agent.ps1"
)
foreach ($f in $Needed) {
  if (-not (Test-Path -LiteralPath (Join-Path $Src $f))) {
    throw "Missing $f. Unzip the whole RongtaPrintAgent folder, then run this script from inside it."
  }
}

$prn = Get-CimInstance Win32_Printer | Where-Object { $_.Name -eq "Rongta RP4xx Series" }
if (-not $prn) { throw "Rongta RP4xx Series is not installed on this PC." }

function Get-BadailPython {
  foreach ($p in @(
      (Join-Path $env:LOCALAPPDATA "Python\pythoncore-3.14-64\python.exe"),
      (Join-Path $env:LOCALAPPDATA "Python\pythoncore-3.13-64\python.exe"),
      (Join-Path $env:LOCALAPPDATA "Python\pythoncore-3.12-64\python.exe"),
      (Join-Path $env:LOCALAPPDATA "Python\bin\python.exe")
    )) {
    if (Test-Path -LiteralPath $p) { return $p }
  }
  $c = Get-Command python -ErrorAction SilentlyContinue
  if ($c -and $c.Source -and ($c.Source -notmatch "WindowsApps")) { return $c.Source }
  $py = Get-Command py -ErrorAction SilentlyContinue
  if ($py) { return $py.Source }
  throw "Python is not installed on this PC."
}

$python = Get-BadailPython
if (-not (Test-Path -LiteralPath $Dest)) {
  New-Item -ItemType Directory -Path $Dest -Force | Out-Null
}
foreach ($f in $Needed) {
  Copy-Item -LiteralPath (Join-Path $Src $f) -Destination (Join-Path $Dest $f) -Force
}
Copy-Item -LiteralPath $PSCommandPath -Destination (Join-Path $Dest "install_rongta_print_agent.ps1") -Force
$Runner = Join-Path $Dest "start_rongta_print_agent.ps1"
$Py = Join-Path $Dest "rongta_local_print.py"

Get-CimInstance Win32_Process | Where-Object {
  $_.CommandLine -and (
    $_.CommandLine -match 'rongta_local_print\.py' -or
    $_.CommandLine -match 'start_rongta_print_agent\.ps1'
  )
} | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

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

$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = $python
if ($python -match '\\py\.exe$') { $psi.Arguments = '-3 -u "' + $Py + '"' }
else { $psi.Arguments = '-u "' + $Py + '"' }
$psi.WorkingDirectory = $Dest
$psi.WindowStyle = [System.Diagnostics.ProcessWindowStyle]::Hidden
$psi.CreateNoWindow = $true
$psi.UseShellExecute = $false
[void][System.Diagnostics.Process]::Start($psi)

$ok = $false
foreach ($i in 1..15) {
  Start-Sleep -Milliseconds 400
  try {
    $r = Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:17891/ping" -TimeoutSec 2
    if ($r.StatusCode -eq 200) { $ok = $true; break }
  } catch {}
}
if (-not $ok) { throw "Print agent did not start on http://127.0.0.1:17891" }
Write-Host "Print agent running on this PC: http://127.0.0.1:17891"
Write-Host "Starts automatically at logon ($TaskName)"
