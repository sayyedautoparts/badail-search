#Requires -Version 5.1
# إنشاء مهمة ويندوز: كل يوم 9:00 صباحاً — رفع قطع سيارات ثم قطع فسب ثم قشطان
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$Runner = Join-Path $PSScriptRoot "run_daily_upload.ps1"
$TaskName = "BadailDailyExcelUpload"
$EnvFile = Join-Path $PSScriptRoot "daily_upload.local.env"

if (-not (Test-Path -LiteralPath $Runner)) {
  throw "Missing runner: $Runner"
}
if (-not (Test-Path -LiteralPath $EnvFile)) {
  Write-Host @"
أنشئ أولاً الملف:
  $EnvFile

محتواه (سطر واحد):
  DATABASE_URL=postgresql://USER:PASS@HOST/DB?sslmode=require

يمكن نسخه من Vercel → Project → Settings → Environment Variables
(NEON_DATABASE_URL أو DATABASE_URL)
"@
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

# بدون Highest حتى لا يحتاج Administrator
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
  # بديل عبر schtasks إن فشل Register-ScheduledTask
  $tr = "09:00"
  schtasks /Create /TN $TaskName /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$Runner`"" /SC DAILY /ST $tr /F /RL LIMITED | Out-Null
  if ($LASTEXITCODE -ne 0) { throw }
}

Write-Host "تم إنشاء/تحديث المهمة: $TaskName"
Write-Host "الوقت: كل يوم 9:00 صباحاً"
Write-Host "التشغيل اليدوي للتجربة:"
Write-Host "  powershell -NoProfile -ExecutionPolicy Bypass -File `"$Runner`""
Write-Host "أو من المجدول: Start-ScheduledTask -TaskName '$TaskName'"
