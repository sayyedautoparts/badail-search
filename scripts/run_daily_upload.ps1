#Requires -Version 5.1
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Py = Join-Path $Root "scripts\daily_local_upload.py"
$EnvFile = Join-Path $Root "scripts\daily_upload.local.env"
$LogDir = Join-Path $Root "scripts\logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

if (Test-Path -LiteralPath $EnvFile) {
  Get-Content -LiteralPath $EnvFile -Encoding UTF8 | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) { return }
    $i = $line.IndexOf("=")
    $k = $line.Substring(0, $i).Trim()
    $v = $line.Substring($i + 1).Trim().Trim("'").Trim('"')
    if ($k) { Set-Item -Path "Env:$k" -Value $v }
  }
}

$python = $null
foreach ($c in @("python", "py")) {
  try {
    $p = Get-Command $c -ErrorAction Stop
    $python = $p.Source
    break
  } catch {}
}
if (-not $python) {
  throw "Python غير موجود في PATH"
}

Set-Location -LiteralPath $Root
& $python $Py @args
exit $LASTEXITCODE
