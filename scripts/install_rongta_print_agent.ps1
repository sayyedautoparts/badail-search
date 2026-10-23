#Requires -Version 5.1
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Runner = Join-Path $PSScriptRoot "start_rongta_print_agent.ps1"
$Py = Join-Path $PSScriptRoot "rongta_local_print.py"
$TaskName = "BadailRongtaPrintAgent"
if (-not (Test-Path -LiteralPath $Runner)) { throw "Missing $Runner" }
if (-not (Test-Path -LiteralPath $Py)) { throw "Missing $Py" }

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
  throw "python not found"
}

$python = Get-BadailPython
$pythonw = $python

Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='pythonw.exe'" -ErrorAction SilentlyContinue |
  Where-Object { $_.CommandLine -and $_.CommandLine -match 'rongta_local_print\.py' } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

$action = New-ScheduledTaskAction `
  -Execute "powershell.exe" `
  -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$Runner`"" `
  -WorkingDirectory $Root
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -StartWhenAvailable `
  -ExecutionTimeLimit (New-TimeSpan -Days 3650)
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
try { Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue } catch {}
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null

try { Remove-ItemProperty -Path "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run" -Name $TaskName -ErrorAction SilentlyContinue } catch {}

$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = $python
$psi.Arguments = '"' + $Py + '"'
$psi.WorkingDirectory = $Root
$psi.WindowStyle = [System.Diagnostics.ProcessWindowStyle]::Hidden
$psi.CreateNoWindow = $true
$psi.UseShellExecute = $false
[void][System.Diagnostics.Process]::Start($psi)
$ok = $false
foreach ($i in 1..12) {
  Start-Sleep -Milliseconds 400
  try {
    $r = Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:17891/ping" -TimeoutSec 2
    if ($r.StatusCode -eq 200) { $ok = $true; break }
  } catch {}
}
if (-not $ok) { throw "Print agent did not start on http://127.0.0.1:17891" }
Write-Host "Print agent running: http://127.0.0.1:17891"
Write-Host "Starts automatically at logon ($TaskName)"
