#Requires -Version 5.1
$ErrorActionPreference = "Stop"
# Whitelist this website in QZ Tray the same way "Allow" does (user folder, no admin needed).
$AllowLine = "A71563FA1D0908B611D6536291AA300554188225	badail-search QZ		2026-10-22 12:50:55	2036-10-20 12:50:55	true"
$QzUser = Join-Path $env:APPDATA "qz"
$AllowFile = Join-Path $QzUser "allowed.dat"

if (-not (Test-Path -LiteralPath $QzUser)) {
  New-Item -ItemType Directory -Path $QzUser -Force | Out-Null
}

$existing = ""
if (Test-Path -LiteralPath $AllowFile) {
  $existing = Get-Content -LiteralPath $AllowFile -Raw -ErrorAction SilentlyContinue
}
if ($existing -notmatch 'A71563FA1D0908B611D6536291AA300554188225') {
  Add-Content -LiteralPath $AllowFile -Value $AllowLine -Encoding ASCII
  Write-Host "Added website to QZ Tray allow list."
} else {
  Write-Host "Website already in QZ Tray allow list."
}

Get-Process | Where-Object { $_.ProcessName -match 'qz' } | ForEach-Object {
  try { Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue } catch {}
}
Start-Sleep -Seconds 1

$exe = Join-Path ${env:ProgramFiles} "QZ Tray\qz-tray.exe"
if (-not (Test-Path -LiteralPath $exe)) {
  $exe = Join-Path ${env:ProgramFiles(x86)} "QZ Tray\qz-tray.exe"
}
if (Test-Path -LiteralPath $exe) {
  Start-Process -FilePath $exe
  Write-Host "QZ Tray restarted."
} else {
  Write-Host "Start QZ Tray from the Start menu."
}

Write-Host ""
Write-Host "OK. Print again. Action Required should be gone." -ForegroundColor Green
Read-Host "Press Enter"
