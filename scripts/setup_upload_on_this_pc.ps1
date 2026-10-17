#Requires -Version 5.1
# Setup daily Excel upload on THIS PC (G:\ملفاتي).
# Usage:
#   powershell -NoProfile -ExecutionPolicy Bypass -File "scripts\setup_upload_on_this_pc.ps1" -AutoYes
param(
  [switch]$AutoYes
)

$ErrorActionPreference = "Stop"
try { [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false) } catch {}

$Root = Split-Path -Parent $PSScriptRoot
$EnvFile = Join-Path $PSScriptRoot "daily_upload.local.env"
$EnvExample = Join-Path $PSScriptRoot "daily_upload.local.env.example"
$InstallTask = Join-Path $PSScriptRoot "install_daily_upload_task.ps1"
$Runner = Join-Path $PSScriptRoot "run_daily_upload.ps1"
$Base = "G:\ملفاتي"

Write-Host "=== Badail daily Excel upload setup ==="
Write-Host ("PC: {0}" -f $env:COMPUTERNAME)
Write-Host ("User: {0}" -f $env:USERNAME)
Write-Host ("Base: {0}" -f $Base)
Write-Host ""

if (-not (Test-Path -LiteralPath $Base)) {
  throw "Missing folder: $Base (Google Drive G: must be available)"
}

$need = @(
  (Join-Path $Base "قطع سيارات"),
  (Join-Path $Base "قطع فسب"),
  (Join-Path $Base "الشغل")
)
foreach ($p in $need) {
  if (-not (Test-Path -LiteralPath $p)) {
    throw "Missing: $p"
  }
  Write-Host "OK: $p"
}

$ordersDir = Join-Path $Base "الشغل\الطلبيات"
$ordersFile = Join-Path $Base "الشغل\الطلبيات.xlsx"
if ((Test-Path -LiteralPath $ordersDir) -or (Test-Path -LiteralPath $ordersFile)) {
  Write-Host "OK: orders (talabat)"
} else {
  Write-Host "WARN: orders folder/file not found yet"
}

if (-not (Test-Path -LiteralPath $EnvFile)) {
  if (Test-Path -LiteralPath $EnvExample) {
    Copy-Item -LiteralPath $EnvExample -Destination $EnvFile
    Write-Host "Created $EnvFile - add NEON_DATABASE_URL then re-run."
    exit 1
  }
  throw "Missing env file: $EnvFile"
}

$envText = Get-Content -LiteralPath $EnvFile -Raw -Encoding UTF8
# يقبل postgresql:// مع أو بدون علامات اقتباس حول القيمة
$dbOk = [regex]::IsMatch($envText, '(?im)^\s*(NEON_DATABASE_URL|DATABASE_URL)\s*=\s*"?postgresql://')
if (-not $dbOk) {
  throw "Set NEON_DATABASE_URL or DATABASE_URL inside daily_upload.local.env"
}

function Set-EnvKey([string]$text, [string]$key, [string]$value) {
  $escapedKey = [regex]::Escape($key)
  $pattern = '(?m)^\s*' + $escapedKey + '\s*=.*$'
  $line = $key + '=' + $value
  if ([regex]::IsMatch($text, $pattern)) {
    return [regex]::Replace($text, $pattern, $line)
  }
  return ($text.TrimEnd() + "`r`n" + $line + "`r`n")
}

$envText = Set-EnvKey $envText "BADAIL_EXCEL_BASE" $Base
$envText = Set-EnvKey $envText "BADAIL_UPLOAD_HOSTS" ""
$utf8Bom = New-Object System.Text.UTF8Encoding $true
[System.IO.File]::WriteAllText($EnvFile, $envText, $utf8Bom)
Write-Host "Env updated (base path + allow this PC)."

function Find-RealPython {
  $candidates = @()
  foreach ($name in @("py", "python", "python3")) {
    $cmd = Get-Command $name -ErrorAction SilentlyContinue
    if ($cmd -and $cmd.Source) { $candidates += $cmd.Source }
  }
  $lad = $env:LOCALAPPDATA
  $pf = ${env:ProgramFiles}
  foreach ($base in @($lad, $pf)) {
    if (-not $base) { continue }
    foreach ($ver in @("Python312", "Python311", "Python310")) {
      $p = Join-Path $base ("Programs\Python\" + $ver + "\python.exe")
      if (Test-Path -LiteralPath $p) { $candidates += $p }
    }
  }
  foreach ($src in $candidates) {
    if (-not $src) { continue }
    if ($src -like '*\WindowsApps\*') { continue }
    if (Test-Path -LiteralPath $src) { return $src }
  }
  return $null
}

$python = Find-RealPython
if (-not $python) {
  throw "Real Python not found. WindowsApps python stub causes exit 9009. Install from https://www.python.org/downloads/ and CHECK 'Add python.exe to PATH'."
}
Write-Host "Python: $python"

$req = Join-Path $Root "requirements.txt"
if (Test-Path -LiteralPath $req) {
  Write-Host "Installing Python packages..."
  if ([IO.Path]::GetFileNameWithoutExtension($python) -ieq "py") {
    & $python -3 -m pip install -q -r $req
  } else {
    & $python -m pip install -q -r $req
  }
}

& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $InstallTask
if ($LASTEXITCODE -ne 0) { throw "Failed to create Windows scheduled task" }

Write-Host ""
Write-Host "Dry-run..."
& powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "& { Set-Location -LiteralPath '$Root'; & '$Runner' '--dry-run' }"

$doUpload = $false
if ($AutoYes) { $doUpload = $true }
if (-not $doUpload) {
  Write-Host ""
  Write-Host "Run real upload now? (Y/N)"
  $ans = Read-Host
  if ($ans -match '^[Yy]') { $doUpload = $true }
}

if ($doUpload) {
  Write-Host "Starting real upload (may take a long time)..."
  & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $Runner
  Write-Host ("Finished with exit code: {0}" -f $LASTEXITCODE)
}

Write-Host ""
Write-Host "Done. Daily upload at 09:00 on this PC:"
Write-Host "  cars + fsb + orders (+ wiper)"
Write-Host ("Manual run: powershell -NoProfile -ExecutionPolicy Bypass -File `"{0}`"" -f $Runner)
