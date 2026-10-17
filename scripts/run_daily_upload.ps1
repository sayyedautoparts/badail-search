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

function Find-RealPython {
  $candidates = @()
  if ($env:BADAIL_PYTHON -and (Test-Path -LiteralPath $env:BADAIL_PYTHON)) {
    $candidates += $env:BADAIL_PYTHON
  }
  $portable = Join-Path $Root "runtime\python\python.exe"
  if (Test-Path -LiteralPath $portable) { $candidates += $portable }

  foreach ($name in @("py", "python", "python3")) {
    $cmd = Get-Command $name -ErrorAction SilentlyContinue
    if ($cmd -and $cmd.Source) { $candidates += $cmd.Source }
  }
  $pf = ${env:ProgramFiles}
  $pf86 = ${env:ProgramFiles(x86)}
  $lad = $env:LOCALAPPDATA
  foreach ($base in @($lad, $pf, $pf86)) {
    if (-not $base) { continue }
    foreach ($p in @(
      (Join-Path $base "Programs\Python\Python312\python.exe"),
      (Join-Path $base "Programs\Python\Python311\python.exe"),
      (Join-Path $base "Programs\Python\Python310\python.exe")
    )) {
      if (Test-Path -LiteralPath $p) { $candidates += $p }
    }
  }
  foreach ($src in $candidates) {
    if (-not $src) { continue }
    if ($src -like '*\WindowsApps\*') { continue }
    if (-not (Test-Path -LiteralPath $src)) { continue }
    return $src
  }
  return $null
}

$python = Find-RealPython
if (-not $python) {
  throw "Python not found. Run: اضغط هنا - نفّذ كل شيء.bat  (downloads portable Python automatically)"
}

Write-Host "Using Python: $python"
Set-Location -LiteralPath $Root
if ([IO.Path]::GetFileNameWithoutExtension($python) -ieq "py") {
  & $python -3 $Py @args
} else {
  & $python $Py @args
}
exit $LASTEXITCODE
