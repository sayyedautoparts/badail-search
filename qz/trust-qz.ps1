#Requires -Version 5.1
$ErrorActionPreference = "Stop"

# QZ only skips "wants to print" when this cert is the app CA (override.crt).
# That file lives next to qz-tray.exe and needs Administrator once.

function Test-Admin {
  $p = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
  return $p.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

if (-not (Test-Admin)) {
  Write-Host "Windows will ask once for Administrator. Click Yes."
  $self = $MyInvocation.MyCommand.Path
  Start-Process -FilePath "powershell.exe" -Verb RunAs -Wait -ArgumentList @(
    "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $self
  )
  exit 0
}

$CertPem = @"
-----BEGIN CERTIFICATE-----
MIIC1zCCAb+gAwIBAgIUS/Iz9EQnumwTaR5bCKCDE6cyNuMwDQYJKoZIhvcNAQEL
BQAwGzEZMBcGA1UEAwwQYmFkYWlsLXNlYXJjaCBRWjAeFw0yNjEwMjIxMjUwNTVa
Fw0zNjEwMjAxMjUwNTVaMBsxGTAXBgNVBAMMEGJhZGFpbC1zZWFyY2ggUVowggEi
MA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCRVVbNwlPHkxcNJtz1Q0lXgxzR
XuOub7GKpGZBfXnFdrkTzfoxXfRPiSFNpg1g9nKpI4XdYY7+w7yRKrnosO55ASWE
x2cnlmWgi6b/evxi2BKZ340OZ8T8W9f3bDZZtaFF2Av7MwMKoZ8W5BkN8Oq7SuIm
5MNCSv8SBj3CYCiDlrrE55KeBuh3ID6bN1lF2tM56dQzls4QKnVUWYuwNGNudAX3
4RhSIzRRKsTClBQTNiL2driIzyPNZub8Pq3L4MAfaA1UYOWeoH6sNznFLcyiRiYV
UFAN8cfTkyutFQuMqzrFpWqUVQdEqmFIFJjNbOefY6SskEx7tukZltrzyCUlAgMB
AAGjEzARMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAFFPm6iG
ef04Abk8qC8tfbYwcGeCqpOeSQlisYw2OTWuY2bRaAygfqOvPK+D9mgGAd+t7IHq
y669jSNX7Lagx50uIMj1A8TO5rRbasFGFq0YOh2L/0Gq6DF7hw25JHwYz0jYnl0L
PW1XcFRbIvAXVNb9wr1C4eYwO3WbB4tL+pQPpvM+isVxi4l2hnRd+EX+xbeyeXSA
N0ikTrqchzRdiW3Mjz0UF8M/ivTJtonbMiDxVgZB+CtFjBDexGKF2mL/wzGenRWl
+hzfVJrSmiMUEHCKX434Paoyro0vn2RyARkbVYZJ6gtvlwjGiairIFcy6zpwylDO
6vAit/5Iz8NKWCo=
-----END CERTIFICATE-----
"@

$qzDir = $null
foreach ($d in @(
  (Join-Path ${env:ProgramFiles} "QZ Tray"),
  (Join-Path ${env:ProgramFiles(x86)} "QZ Tray")
)) {
  if (Test-Path -LiteralPath (Join-Path $d "qz-tray.exe")) { $qzDir = $d; break }
}
if (-not $qzDir) {
  Write-Host "ERROR: QZ Tray is not installed." -ForegroundColor Red
  Read-Host "Press Enter"
  exit 1
}

$override = Join-Path $qzDir "override.crt"
Set-Content -LiteralPath $override -Value $CertPem.Trim() -Encoding ASCII
Write-Host "Installed: $override"

$props = Join-Path $qzDir "qz-tray.properties"
$propLine = "authcert.override=" + ($override -replace '\\', '\\')
if (Test-Path -LiteralPath $props) {
  $txt = Get-Content -LiteralPath $props -Raw -ErrorAction SilentlyContinue
  if ($txt -notmatch 'authcert\.override=') {
    Add-Content -LiteralPath $props -Value $propLine
  }
} else {
  Set-Content -LiteralPath $props -Value $propLine -Encoding ASCII
}

$AllowLine = "A71563FA1D0908B611D6536291AA300554188225	badail-search QZ		2026-10-22 12:50:55	2036-10-20 12:50:55	true"
foreach ($userQz in @(
  (Join-Path $env:APPDATA "qz"),
  (Join-Path $env:ProgramData "qz")
)) {
  if (-not (Test-Path -LiteralPath $userQz)) {
    New-Item -ItemType Directory -Path $userQz -Force | Out-Null
  }
  $allowFile = Join-Path $userQz "allowed.dat"
  $existing = ""
  if (Test-Path -LiteralPath $allowFile) {
    $existing = Get-Content -LiteralPath $allowFile -Raw -ErrorAction SilentlyContinue
  }
  if ($existing -notmatch 'A71563FA1D0908B611D6536291AA300554188225') {
    Add-Content -LiteralPath $allowFile -Value $AllowLine -Encoding ASCII
  }
}

Get-Process | Where-Object { $_.ProcessName -match 'qz' } | ForEach-Object {
  try { Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue } catch {}
}
Start-Sleep -Seconds 2
Start-Process -FilePath (Join-Path $qzDir "qz-tray.exe")

Write-Host ""
Write-Host "OK. Restarted QZ Tray. Print again — no Action Required." -ForegroundColor Green
Read-Host "Press Enter"
