#Requires -Version 5.1
# Create Windows scheduled task: daily 09:00 Excel upload
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$Runner = Join-Path $PSScriptRoot "run_daily_upload.ps1"
$TaskName = "BadailDailyExcelUpload"
$EnvFile = Join-Path $PSScriptRoot "daily_upload.local.env"

if (-not (Test-Path -LiteralPath $Runner)) {
  throw "Missing runner: $Runner"
}
if (-not (Test-Path -LiteralPath $EnvFile)) {
  Write-Host "Missing env file:"
  Write-Host "  $EnvFile"
  Write-Host "Add NEON_DATABASE_URL=postgresql://..."
  exit 1
}

$action = New-ScheduledTaskAction `
  -Execute "powershell.exe" `
  -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$Runner`"" `
  -WorkingDirectory $Root

$trigger = New-ScheduledTaskTrigger -Daily -At 9:00am

$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -StartWhenAvailable `
  -WakeToRun `
  -ExecutionTimeLimit (New-TimeSpan -Hours 6)

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

try {
  Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null
} catch {
  $tr = "09:00"
  schtasks /Create /TN $TaskName /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$Runner`"" /SC DAILY /ST $tr /F /RL LIMITED | Out-Null
  if ($LASTEXITCODE -ne 0) { throw }
}

Write-Host "Scheduled task ready: $TaskName"
Write-Host "Time: every day 09:00"
Write-Host ("PC: {0} / User: {1}" -f $env:COMPUTERNAME, $env:USERNAME)
Write-Host ("Manual: powershell -NoProfile -ExecutionPolicy Bypass -File `"{0}`"" -f $Runner)
Write-Host ("Or: Start-ScheduledTask -TaskName '{0}'" -f $TaskName)
