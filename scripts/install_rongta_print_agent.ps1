#Requires -Version 5.1
# Run only on the PC with the Rongta USB printer.
$ErrorActionPreference = "Stop"
$Worker = Join-Path $PSScriptRoot "rongta_print_worker.ps1"
$PrintPs1 = Join-Path $PSScriptRoot "print_rongta_image.ps1"
$TaskName = "BadailRongtaPrintAgent"
if (-not (Test-Path -LiteralPath $Worker)) { throw "Missing $Worker" }
if (-not (Test-Path -LiteralPath $PrintPs1)) { throw "Missing $PrintPs1" }

$prn = Get-WmiObject Win32_Printer | Where-Object { $_.Name -eq "Rongta RP4xx Series" }
if (-not $prn) {
  throw "Rongta RP4xx Series is not installed on this PC."
}

Get-CimInstance Win32_Process | Where-Object {
  $_.CommandLine -and (
    $_.CommandLine -match 'rongta_local_print\.py' -or
    $_.CommandLine -match 'rongta_print_worker\.ps1' -or
    $_.CommandLine -match 'start_rongta_print_agent\.ps1'
  )
} | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }

$action = New-ScheduledTaskAction `
  -Execute "powershell.exe" `
  -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$Worker`"" `
  -WorkingDirectory $PSScriptRoot
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -StartWhenAvailable `
  -ExecutionTimeLimit (New-TimeSpan -Days 3650)
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited
try { Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue } catch {}
Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName
$ok = $false
foreach ($i in 1..20) {
  Start-Sleep -Milliseconds 400
  try {
    $r = Invoke-WebRequest -UseBasicParsing "http://127.0.0.1:17891/ping" -TimeoutSec 2
    if ($r.StatusCode -eq 200) { $ok = $true; break }
  } catch {}
}
if (-not $ok) { throw "Failed to start local print server on 127.0.0.1:17891" }
Write-Host "Rongta on this PC will print directly ($env:COMPUTERNAME)."
Write-Host "Open the shop website ON THIS computer, then press Print."
Write-Host "Starts automatically at logon ($TaskName)"
