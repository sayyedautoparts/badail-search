#Requires -Version 5.1
# شغّل هذا الملف فقط على الجهاز اللي شابك فيه طابعة Rongta.
$ErrorActionPreference = "Stop"
$Worker = Join-Path $PSScriptRoot "rongta_print_worker.ps1"
$PrintPs1 = Join-Path $PSScriptRoot "print_rongta_image.ps1"
$TaskName = "BadailRongtaPrintAgent"
if (-not (Test-Path -LiteralPath $Worker)) { throw "Missing $Worker" }
if (-not (Test-Path -LiteralPath $PrintPs1)) { throw "Missing $PrintPs1" }

$prn = Get-WmiObject Win32_Printer | Where-Object { $_.Name -eq "Rongta RP4xx Series" }
if (-not $prn) {
  throw "طابعة Rongta RP4xx Series غير موجودة على هذا الجهاز. ثبّت الدرايفر هنا ثم أعد المحاولة."
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
Start-Sleep -Seconds 2
Write-Host "Print worker is running on this PC ($env:COMPUTERNAME)."
Write-Host "Only this machine (with Rongta USB) will print shop labels."
Write-Host "Starts automatically at logon ($TaskName)"
