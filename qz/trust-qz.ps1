#Requires -Version 5.1
$ErrorActionPreference = "Stop"
$CertUrl = "https://badail-search.vercel.app/qz/cert"

function Test-Admin {
  $p = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
  return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-Admin)) {
  Write-Host "Need Administrator once to trust this website in QZ Tray..."
  $self = $MyInvocation.MyCommand.Path
  Start-Process -FilePath "powershell.exe" -Verb RunAs -ArgumentList @(
    "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $self
  )
  exit 0
}

$tmp = Join-Path $env:TEMP "badail-qz.crt"
Write-Host "Downloading certificate..."
Invoke-WebRequest -UseBasicParsing -Uri $CertUrl -OutFile $tmp

$qzDirs = @(
  (Join-Path ${env:ProgramFiles} "QZ Tray"),
  (Join-Path ${env:ProgramFiles(x86)} "QZ Tray")
)
$qzDir = $null
foreach ($d in $qzDirs) {
  if (Test-Path -LiteralPath $d) { $qzDir = $d; break }
}
if (-not $qzDir) {
  Write-Host "ERROR: QZ Tray folder not found. Install QZ Tray first." -ForegroundColor Red
  Read-Host "Press Enter"
  exit 1
}

Get-Process | Where-Object { $_.ProcessName -match 'qz' } | ForEach-Object {
  try { Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue } catch {}
}
Start-Sleep -Seconds 1

$override = Join-Path $qzDir "override.crt"
Copy-Item -LiteralPath $tmp -Destination $override -Force
Write-Host "Installed: $override"

$props = Join-Path $qzDir "qz-tray.properties"
$line = "authcert.override=" + ($override -replace '\\', '\\')
if (Test-Path -LiteralPath $props) {
  $txt = Get-Content -LiteralPath $props -Raw -ErrorAction SilentlyContinue
  if ($txt -notmatch 'authcert\.override=') {
    Add-Content -LiteralPath $props -Value $line
  }
} else {
  Set-Content -LiteralPath $props -Value $line -Encoding ASCII
}

$appDataQz = Join-Path $env:APPDATA "qz"
if (-not (Test-Path -LiteralPath $appDataQz)) {
  New-Item -ItemType Directory -Path $appDataQz -Force | Out-Null
}
Copy-Item -LiteralPath $tmp -Destination (Join-Path $appDataQz "override.crt") -Force

$jar = Join-Path $qzDir "qz-tray.jar"
$java = Join-Path $qzDir "runtime\bin\java.exe"
if (-not (Test-Path -LiteralPath $java)) {
  $java = Join-Path $qzDir "jre\bin\java.exe"
}
if ((Test-Path -LiteralPath $java) -and (Test-Path -LiteralPath $jar)) {
  try {
    & $java -jar $jar --allow $tmp
    Write-Host "Certificate added to QZ allow list."
  } catch {
    Write-Host "Note: --allow failed: $($_.Exception.Message)"
  }
}

$exe = Join-Path $qzDir "qz-tray.exe"
if (Test-Path -LiteralPath $exe) {
  Start-Process -FilePath $exe
}

Write-Host ""
Write-Host "OK. QZ Tray trusted this website." -ForegroundColor Green
Write-Host "Print again. Action Required should stop."
Read-Host "Press Enter"
