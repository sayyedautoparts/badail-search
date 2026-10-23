#Requires -Version 5.1
# Run this ONLY on the PC that has the Rongta USB printer.
$ErrorActionPreference = "Continue"
$PrintPs1 = Join-Path $PSScriptRoot "print_rongta_image.ps1"
$LogDir = Join-Path $env:LOCALAPPDATA "BadailPrint"
$Log = Join-Path $LogDir "agent.log"
$Key = "badail-rongta"
$Sites = @("https://badail-search.vercel.app")
$Curl = Join-Path $env:SystemRoot "System32\curl.exe"
$Printer = "Rongta RP4xx Series"
if (-not (Test-Path -LiteralPath $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

function Write-AgentLog([string]$Message) {
  $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
  Add-Content -LiteralPath $Log -Value $line -Encoding UTF8
}

function Invoke-PrintApi {
  param([string]$Method, [string]$Path, [string]$Body)
  $last = $null
  foreach ($site in $Sites) {
    $url = $site.TrimEnd("/") + $Path
    try {
      $args = @("-sS", "-m", "25", "-H", "X-Print-Key: $Key", "-H", "Accept: application/json", "-X", $Method, $url)
      if ($null -ne $Body) {
        $args = @("-sS", "-m", "25", "-H", "X-Print-Key: $Key", "-H", "Accept: application/json", "-H", "Content-Type: application/json", "-X", $Method, "--data-binary", $Body, $url)
      }
      $out = & $Curl @args 2>&1 | Out-String
      if ($LASTEXITCODE -ne 0) { throw $out }
      if (-not $out.Trim()) { return $null }
      return ($out | ConvertFrom-Json)
    } catch {
      $last = $_
    }
  }
  throw $last
}

Write-AgentLog "print worker started on $env:COMPUTERNAME"
while ($true) {
  try {
    $data = Invoke-PrintApi -Method GET -Path "/print_jobs/next" -Body $null
    $job = $data.job
    if (-not $job) { Start-Sleep -Seconds 2; continue }
    $tmp = Join-Path $env:TEMP ("badail-print-" + $job.id + ".png")
    $bytes = [Convert]::FromBase64String([string]$job.image)
    [IO.File]::WriteAllBytes($tmp, $bytes)
    try {
      & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $PrintPs1 -ImagePath $tmp -PrinterName $Printer -Copies ([int]$job.copies) -WidthMm ([double]$job.widthMm) -HeightMm ([double]$job.heightMm)
      if ($LASTEXITCODE -ne 0) { throw "print exit $LASTEXITCODE" }
      Invoke-PrintApi -Method POST -Path "/print_jobs/ack" -Body (@{ id = [int]$job.id; ok = $true } | ConvertTo-Json -Compress) | Out-Null
      Write-AgentLog ("printed job " + $job.id + " x" + $job.copies + " on " + $env:COMPUTERNAME)
    } catch {
      $msg = [string]$_.Exception.Message
      try { Invoke-PrintApi -Method POST -Path "/print_jobs/ack" -Body (@{ id = [int]$job.id; ok = $false; message = $msg } | ConvertTo-Json -Compress) | Out-Null } catch {}
      Write-AgentLog ("job " + $job.id + " failed: " + $msg)
    } finally {
      Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
  } catch {
    Write-AgentLog ("poll error: " + $_.Exception.Message)
    Start-Sleep -Seconds 4
  }
}
