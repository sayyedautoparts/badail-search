#Requires -Version 5.1
<#
  تثبيت الرفع اليومي التلقائي على الجهاز الحالي (المقصود: جهاز Asim).
  المسار الافتراضي: G:\ملفاتي
    - قطع سيارات\
    - قطع فسب\
    - الشغل\قشطان.xlsx

  شغّله مرة واحدة على جهاز Asim (PowerShell):
    powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\setup_upload_on_this_pc.ps1"
#>
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [Text.UTF8Encoding]::UTF8

$Root = Split-Path -Parent $PSScriptRoot
$EnvFile = Join-Path $PSScriptRoot "daily_upload.local.env"
$EnvExample = Join-Path $PSScriptRoot "daily_upload.local.env.example"
$InstallTask = Join-Path $PSScriptRoot "install_daily_upload_task.ps1"
$Runner = Join-Path $PSScriptRoot "run_daily_upload.ps1"
$Base = "G:\ملفاتي"

Write-Host "=== تثبيت رفع الإكسل التلقائي ==="
Write-Host ("الجهاز: {0}" -f $env:COMPUTERNAME)
Write-Host ("المستخدم: {0}" -f $env:USERNAME)
Write-Host ("المسار: {0}" -f $Base)
Write-Host ""

if (-not (Test-Path -LiteralPath $Base)) {
  throw "المجلد غير موجود: $Base — تأكد أن قرص G: مربوط على هذا الجهاز."
}

$need = @(
  (Join-Path $Base "قطع سيارات"),
  (Join-Path $Base "قطع فسب"),
  (Join-Path $Base "الشغل\قشطان.xlsx")
)
foreach ($p in $need) {
  if (-not (Test-Path -LiteralPath $p)) {
    throw "ناقص: $p"
  }
  Write-Host "OK: $p"
}

if (-not (Test-Path -LiteralPath $EnvFile)) {
  if (Test-Path -LiteralPath $EnvExample) {
    Copy-Item -LiteralPath $EnvExample -Destination $EnvFile
    Write-Host ""
    Write-Host "تم إنشاء $EnvFile من المثال."
    Write-Host "افتح الملف وضع NEON_DATABASE_URL ثم أعد تشغيل هذا السكربت."
    exit 1
  }
  throw "ناقص ملف البيئة: $EnvFile"
}

# تأكد من وجود رابط قاعدة البيانات
$envText = Get-Content -LiteralPath $EnvFile -Raw -Encoding UTF8
if ($envText -notmatch 'NEON_DATABASE_URL\s*=\s*postgresql://' -and $envText -notmatch 'DATABASE_URL\s*=\s*postgresql://') {
  throw "ضع NEON_DATABASE_URL أو DATABASE_URL داخل $EnvFile"
}

# حدّث/أضف إعدادات الجهاز والمسار دون حذف باقي المتغيرات
function Set-EnvKey([string]$text, [string]$key, [string]$value) {
  $pattern = "(?m)^\s*$([regex]::Escape($key))\s*=.*$"
  $line = "$key=$value"
  if ($text -match $pattern) {
    return [regex]::Replace($text, $pattern, $line)
  }
  return ($text.TrimEnd() + "`r`n$key=$value`r`n")
}

$hostTokens = @($env:COMPUTERNAME, $env:USERNAME, "Asim", "sayyed") |
  Where-Object { $_ } |
  ForEach-Object { $_.ToString().Trim() } |
  Select-Object -Unique
$hostList = ($hostTokens -join ",")

$envText = Set-EnvKey $envText "BADAIL_EXCEL_BASE" $Base
$envText = Set-EnvKey $envText "BADAIL_UPLOAD_HOSTS" $hostList
Set-Content -LiteralPath $EnvFile -Value $envText -Encoding UTF8
Write-Host "تم تحديث BADAIL_EXCEL_BASE و BADAIL_UPLOAD_HOSTS في ملف البيئة."

# Python + متطلبات أساسية
$python = $null
foreach ($c in @("python", "py")) {
  try {
    $p = Get-Command $c -ErrorAction Stop
    $python = $p.Source
    break
  } catch {}
}
if (-not $python) {
  throw "ثبّت Python 3 وأضفه إلى PATH ثم أعد المحاولة."
}
Write-Host "Python: $python"

$req = Join-Path $Root "requirements.txt"
if (Test-Path -LiteralPath $req) {
  Write-Host "تثبيت متطلبات Python (إن لزم)..."
  & $python -m pip install -q -r $req
}

# مهمة يومية 9:00
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $InstallTask
if ($LASTEXITCODE -ne 0) { throw "فشل إنشاء مهمة ويندوز" }

Write-Host ""
Write-Host "تجربة جافة (بدون رفع)..."
& powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "& { Set-Location -LiteralPath '$Root'; & '$Runner' '--dry-run' }"
if ($LASTEXITCODE -ne 0) {
  Write-Host "تحذير: dry-run رجع كود $LASTEXITCODE — راجع السجل في scripts\logs"
}

Write-Host ""
Write-Host "تشغيل رفع حقيقي الآن؟ (Y/N)"
$ans = Read-Host
if ($ans -match '^[Yyنعم]') {
  Write-Host "جاري الرفع الحقيقي..."
  & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $Runner
  Write-Host ("انتهى بكود: {0}" -f $LASTEXITCODE)
}

Write-Host ""
Write-Host "تم. الرفع التلقائي كل يوم 9:00 صباحاً على هذا الجهاز."
Write-Host "للتجربة اليدوية:"
Write-Host "  powershell -NoProfile -ExecutionPolicy Bypass -File `"$Runner`""
