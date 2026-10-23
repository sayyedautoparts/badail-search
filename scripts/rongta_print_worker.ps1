#Requires -Version 5.1
# Local print server: browser on THIS PC -> Rongta USB on THIS PC.
$ErrorActionPreference = "Stop"
$PrintPs1 = Join-Path $PSScriptRoot "print_rongta_image.ps1"
$LogDir = Join-Path $env:LOCALAPPDATA "BadailPrint"
$Log = Join-Path $LogDir "agent.log"
$Printer = "Rongta RP4xx Series"
$Port = 17891
if (-not (Test-Path -LiteralPath $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

function Write-AgentLog([string]$Message) {
  $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
  try { Add-Content -LiteralPath $Log -Value $line -Encoding UTF8 } catch {}
}

function Send-Http($stream, [int]$code, [string]$status, [string]$ctype, [byte[]]$body, [string]$origin) {
  if (-not $origin) { $origin = "*" }
  $head = "HTTP/1.1 $code $status`r`nAccess-Control-Allow-Origin: $origin`r`nAccess-Control-Allow-Methods: GET, POST, OPTIONS`r`nAccess-Control-Allow-Headers: Content-Type, Access-Control-Request-Private-Network, Access-Control-Request-Local-Network`r`nAccess-Control-Allow-Private-Network: true`r`nAccess-Control-Allow-Local-Network: true`r`nAccess-Control-Max-Age: 600`r`nVary: Origin`r`nContent-Type: $ctype`r`nContent-Length: $($body.Length)`r`nConnection: close`r`n`r`n"
  $hb = [Text.Encoding]::ASCII.GetBytes($head)
  $stream.Write($hb, 0, $hb.Length)
  if ($body.Length -gt 0) { $stream.Write($body, 0, $body.Length) }
  $stream.Flush()
}

function Read-HttpRequest($stream) {
  $ms = New-Object IO.MemoryStream
  $buf = New-Object byte[] 8192
  $raw = $null
  while ($true) {
    $n = $stream.Read($buf, 0, $buf.Length)
    if ($n -le 0) { break }
    $ms.Write($buf, 0, $n)
    $raw = [Text.Encoding]::ASCII.GetString($ms.ToArray())
    $idx = $raw.IndexOf("`r`n`r`n")
    if ($idx -lt 0) { continue }
    $headerText = $raw.Substring(0, $idx)
    $cl = 0
    foreach ($line in $headerText -split "`r`n") {
      if ($line -match '^(?i)Content-Length:\s*(\d+)') { $cl = [int]$Matches[1] }
    }
    $got = $ms.Length - ($idx + 4)
    while ($got -lt $cl) {
      $n = $stream.Read($buf, 0, $buf.Length)
      if ($n -le 0) { break }
      $ms.Write($buf, 0, $n)
      $got += $n
    }
    break
  }
  $bytes = $ms.ToArray()
  $text = [Text.Encoding]::UTF8.GetString($bytes)
  $idx = $text.IndexOf("`r`n`r`n")
  if ($idx -lt 0) { return $null }
  $headerText = $text.Substring(0, $idx)
  $body = $text.Substring($idx + 4)
  $lines = $headerText -split "`r`n"
  $req = $lines[0] -split " "
  $origin = ""
  foreach ($line in $lines) {
    if ($line -match '^(?i)Origin:\s*(.+)$') { $origin = $Matches[1].Trim() }
  }
  return @{
    Method = $req[0]
    Path = (($req[1] -split "\?")[0])
    Body = $body
    Origin = $origin
  }
}

function Invoke-OneCloudJob() {
  $key = "badail-rongta"
  $sites = @("https://badail-search.vercel.app", "https://www.sayyedstore.com")
  $job = $null
  foreach ($site in $sites) {
    try {
      $r = Invoke-WebRequest -UseBasicParsing -Headers @{ "X-Print-Key" = $key } -Uri ($site + "/print_jobs/next") -TimeoutSec 20
      $data = $r.Content | ConvertFrom-Json
      if ($data.job) { $job = $data.job; break }
    } catch {}
  }
  if (-not $job) {
    return @{ ok = $false; title = "لا يوجد أمر طباعة"; msg = "ما في ملصق بالانتظار على هذا الجهاز." }
  }
  $copies = 1
  try { $copies = [Math]::Max(1, [Math]::Min(50, [int]$job.copies)) } catch { $copies = 1 }
  $w = 105; if ($job.widthMm) { $w = [double]$job.widthMm }
  $h = 28; if ($job.heightMm) { $h = [double]$job.heightMm }
  Invoke-RongtaPrint -ImageB64 ([string]$job.image) -Copies $copies -WidthMm $w -HeightMm $h
  foreach ($site in $sites) {
    try {
      $body = (@{ id = [int]$job.id; ok = $true } | ConvertTo-Json -Compress)
      Invoke-WebRequest -UseBasicParsing -Method POST -Headers @{ "X-Print-Key" = $key; "Content-Type" = "application/json" } -Uri ($site + "/print_jobs/ack") -Body $body -TimeoutSec 15 | Out-Null
      break
    } catch {}
  }
  Write-AgentLog "printed job $($job.id) on $env:COMPUTERNAME"
  return @{ ok = $true; title = "تمت الطباعة"; msg = ("Rongta على هذا الجهاز · {0} ورقة" -f $copies) }
}

function Invoke-RongtaPrint([string]$ImageB64, [int]$Copies, [double]$WidthMm, [double]$HeightMm) {
  if ($ImageB64 -match ",") { $ImageB64 = $ImageB64.Split(",", 2)[1] }
  $blob = [Convert]::FromBase64String($ImageB64)
  if ($blob.Length -lt 32) { throw "empty image" }
  $tmp = Join-Path $env:TEMP ("badail-" + [guid]::NewGuid().ToString("N") + ".png")
  [IO.File]::WriteAllBytes($tmp, $blob)
  try {
    $p = Start-Process -FilePath "powershell.exe" -WorkingDirectory $PSScriptRoot -ArgumentList @(
      "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "print_rongta_image.ps1",
      "-ImagePath", $tmp, "-PrinterName", $Printer, "-Copies", "$Copies",
      "-WidthMm", "$WidthMm", "-HeightMm", "$HeightMm"
    ) -Wait -PassThru -WindowStyle Hidden
    if ($p.ExitCode -ne 0) { throw "print exit $($p.ExitCode)" }
  } finally {
    Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
  }
}

Write-AgentLog "direct Rongta listener on 127.0.0.1:$Port ($env:COMPUTERNAME)"
$server = New-Object System.Net.Sockets.TcpListener ([Net.IPAddress]::Loopback), $Port
$server.Start()
try {
  while ($true) {
    $client = $server.AcceptTcpClient()
    try {
      $stream = $client.GetStream()
      $stream.ReadTimeout = 30000
      $req = Read-HttpRequest $stream
      if (-not $req) { continue }
      $origin = $req.Origin
      $jsonUtf8 = New-Object System.Text.UTF8Encoding $false
      if ($req.Method -eq "OPTIONS") {
        Send-Http $stream 204 "No Content" "text/plain" ([byte[]]@()) $origin
      } elseif ($req.Path -in @("/ping", "/health") -and $req.Method -eq "GET") {
        $b = $jsonUtf8.GetBytes((@{ ok = $true; printer = $Printer; host = $env:COMPUTERNAME } | ConvertTo-Json -Compress))
        Send-Http $stream 200 "OK" "application/json; charset=utf-8" $b $origin
      } elseif ($req.Path -in @("/go", "/print_go") -and $req.Method -eq "GET") {
        try {
          $r = Invoke-OneCloudJob
          $html = "<!doctype html><meta charset=utf-8><title>Print</title><body style='font-family:Arial;padding:24px;direction:rtl;text-align:center'><h2>$($r.title)</h2><p>$($r.msg)</p><script>setTimeout(function(){window.close()},2500)</script></body>"
          $b = $jsonUtf8.GetBytes($html)
          Send-Http $stream 200 "OK" "text/html; charset=utf-8" $b $origin
        } catch {
          $msg = [string]$_.Exception.Message
          Write-AgentLog "go fail: $msg"
          $html = "<!doctype html><meta charset=utf-8><title>Print</title><body style='font-family:Arial;padding:24px;direction:rtl;text-align:center'><h2>فشلت الطباعة</h2><p>$msg</p></body>"
          $b = $jsonUtf8.GetBytes($html)
          Send-Http $stream 200 "OK" "text/html; charset=utf-8" $b $origin
        }
      } elseif ($req.Path -eq "/print" -and $req.Method -eq "POST") {
        try {
          $payload = $req.Body | ConvertFrom-Json
          $copies = [Math]::Max(1, [Math]::Min(50, [int]($payload.copies)))
          if ($copies -lt 1) { $copies = 1 }
          $w = 105; if ($payload.widthMm) { $w = [double]$payload.widthMm }
          $h = 28; if ($payload.heightMm) { $h = [double]$payload.heightMm }
          Invoke-RongtaPrint -ImageB64 ([string]$payload.image) -Copies $copies -WidthMm $w -HeightMm $h
          $b = $jsonUtf8.GetBytes((@{ ok = $true; copies = $copies; printer = $Printer } | ConvertTo-Json -Compress))
          Send-Http $stream 200 "OK" "application/json; charset=utf-8" $b $origin
          Write-AgentLog "printed $copies copy on $Printer"
        } catch {
          $msg = [string]$_.Exception.Message
          Write-AgentLog "print fail: $msg"
          $b = $jsonUtf8.GetBytes((@{ ok = $false; message = $msg } | ConvertTo-Json -Compress))
          Send-Http $stream 500 "Error" "application/json; charset=utf-8" $b $origin
        }
      } else {
        Send-Http $stream 404 "Not Found" "text/plain" ([byte[]]@()) $origin
      }
    } catch {
      Write-AgentLog ("http error: " + $_.Exception.Message)
    } finally {
      try { $client.Close() } catch {}
    }
  }
} finally {
  $server.Stop()
}
