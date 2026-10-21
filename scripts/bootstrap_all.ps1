#Requires -Version 5.1
# One-click: ensure portable Python, install deps, schedule task, run upload.
$ErrorActionPreference = "Stop"
try { [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false) } catch {}

$Root = Split-Path -Parent $PSScriptRoot
$RuntimeDir = Join-Path $Root "runtime\python"
$PythonExe = Join-Path $RuntimeDir "python.exe"
$EnvFile = Join-Path $PSScriptRoot "daily_upload.local.env"
$Req = Join-Path $Root "requirements.txt"
$InstallTask = Join-Path $PSScriptRoot "install_daily_upload_task.ps1"
$Runner = Join-Path $PSScriptRoot "run_daily_upload.ps1"
$PyVer = "3.12.7"
$PyZipUrl = "https://www.python.org/ftp/python/$PyVer/python-$PyVer-embed-amd64.zip"
$GetPipUrl = "https://bootstrap.pypa.io/get-pip.py"

Write-Host "============================================"
Write-Host " Badail ONE-CLICK setup + upload"
Write-Host "============================================"
Write-Host ("PC: {0}" -f $env:COMPUTERNAME)
Write-Host ("Root: {0}" -f $Root)
Write-Host ""

function Test-DbEnv([string]$path) {
  if (-not (Test-Path -LiteralPath $path)) { return $false }
  $t = Get-Content -LiteralPath $path -Raw -Encoding UTF8
  return [regex]::IsMatch($t, '(?im)^\s*(NEON_DATABASE_URL|DATABASE_URL)\s*=\s*"?postgresql://')
}

if (-not (Test-DbEnv $EnvFile)) {
  throw "Missing NEON_DATABASE_URL in scripts\daily_upload.local.env"
}

# --- Portable Python ---
if (-not (Test-Path -LiteralPath $PythonExe)) {
  Write-Host "Downloading portable Python $PyVer ..."
  $tmp = Join-Path $env:TEMP ("python-embed-" + $PyVer + ".zip")
  New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null
  [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
  Invoke-WebRequest -Uri $PyZipUrl -OutFile $tmp -UseBasicParsing
  Expand-Archive -LiteralPath $tmp -DestinationPath $RuntimeDir -Force
  Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue

  # enable site-packages in embeddable
  $pth = Get-ChildItem -LiteralPath $RuntimeDir -Filter "python*._pth" | Select-Object -First 1
  if ($pth) {
    $lines = Get-Content -LiteralPath $pth.FullName
    $out = @()
    foreach ($line in $lines) {
      if ($line -match '^\s*#\s*import\s+site\s*$') { $out += "import site" }
      else { $out += $line }
    }
    if (-not ($out -contains "import site")) { $out += "import site" }
    Set-Content -LiteralPath $pth.FullName -Value $out -Encoding ASCII
  }
}

if (-not (Test-Path -LiteralPath $PythonExe)) {
  throw "Portable python.exe missing after download"
}
Write-Host "Python OK: $PythonExe"

# --- pip + requirements ---
Write-Host "Ensuring pip + packages..."
$getPip = Join-Path $RuntimeDir "get-pip.py"
if (-not (Test-Path -LiteralPath (Join-Path $RuntimeDir "Scripts\pip.exe"))) {
  [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
  Invoke-WebRequest -Uri $GetPipUrl -OutFile $getPip -UseBasicParsing
  & $PythonExe $getPip --no-warn-script-location
}
& $PythonExe -m pip install -q --upgrade pip
if (Test-Path -LiteralPath $Req) {
  & $PythonExe -m pip install -q -r $Req
}

# Prefer this portable python for runner/task
$env:BADAIL_PYTHON = $PythonExe

# Point env base
$envText = Get-Content -LiteralPath $EnvFile -Raw -Encoding UTF8
function Set-EnvKey([string]$text, [string]$key, [string]$value) {
  $escapedKey = [regex]::Escape($key)
  $pattern = '(?m)^\s*' + $escapedKey + '\s*=.*$'
  $line = $key + '=' + $value
  if ([regex]::IsMatch($text, $pattern)) { return [regex]::Replace($text, $pattern, $line) }
  return ($text.TrimEnd() + "`r`n" + $line + "`r`n")
}
$envText = Set-EnvKey $envText "BADAIL_EXCEL_BASE" "G:\ملفاتي"
$envText = Set-EnvKey $envText "BADAIL_UPLOAD_HOSTS" ""
$utf8Bom = New-Object System.Text.UTF8Encoding $true
[System.IO.File]::WriteAllText($EnvFile, $envText, $utf8Bom)

Write-Host "Creating 10-day scheduled task (09:00, first run tomorrow)..."
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $InstallTask
if ($LASTEXITCODE -ne 0) { throw "Scheduled task failed" }

Write-Host ""
Write-Host "Starting FULL upload now (cars + fsb + orders + wiper)..."
Write-Host "This can take many minutes. Leave window open."
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $Runner
$code = $LASTEXITCODE
Write-Host ""
Write-Host ("Finished upload exit code: {0}" -f $code)
Write-Host "Auto Excel upload is set every 10 days at 09:00 (first run tomorrow)."
if ($code -ne 0) { exit $code }
exit 0
