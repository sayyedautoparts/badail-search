#Requires -Version 5.1
# Create Windows scheduled task: Excel upload every 10 days at 09:00
# First run: tomorrow 09:00
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
  -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$Runner`"" `
  -WorkingDirectory $Root

$startAt = [datetime]::Today.AddDays(1).AddHours(9)

$settings = New-ScheduledTaskSettingsSet `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -StartWhenAvailable `
  -WakeToRun `
  -ExecutionTimeLimit (New-TimeSpan -Hours 6)

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

try {
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
} catch {}

$registered = $false
try {
  $trigger = New-ScheduledTaskTrigger -Once -At $startAt -RepetitionInterval (New-TimeSpan -Days 10) -RepetitionDuration (New-TimeSpan -Days 9999)
  Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Force | Out-Null
  $registered = $true
} catch {
  $registered = $false
}

if (-not $registered) {
  $sd = $startAt.ToString("dd/MM/yyyy")
  schtasks /Create /TN $TaskName /TR "powershell.exe -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$Runner`"" /SC DAILY /MO 10 /ST 09:00 /SD $sd /F /RL LIMITED | Out-Null
  if ($LASTEXITCODE -ne 0) {
    $sdUs = $startAt.ToString("MM/dd/yyyy")
    schtasks /Create /TN $TaskName /TR "powershell.exe -NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$Runner`"" /SC DAILY /MO 10 /ST 09:00 /SD $sdUs /F /RL LIMITED | Out-Null
  }
  if ($LASTEXITCODE -ne 0) { throw "Failed to create scheduled task $TaskName" }
}

Write-Host "Scheduled task ready: $TaskName"
Write-Host ("First run: {0:yyyy-MM-dd} 09:00" -f $startAt)
Write-Host "Repeat: every 10 days at 09:00"
Write-Host ("PC: {0} / User: {1}" -f $env:COMPUTERNAME, $env:USERNAME)
Write-Host ("Manual: powershell -NoProfile -ExecutionPolicy Bypass -File `"{0}`"" -f $Runner)
Write-Host ("Or: Start-ScheduledTask -TaskName '{0}'" -f $TaskName)
