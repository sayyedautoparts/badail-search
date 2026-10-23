#Requires -Version 5.1
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Py = Join-Path $PSScriptRoot "rongta_local_print.py"
$LogDir = Join-Path $env:LOCALAPPDATA "BadailPrint"
$Log = Join-Path $LogDir "agent.log"
if (-not (Test-Path -LiteralPath $LogDir)) {
  New-Item -ItemType Directory -Path $LogDir | Out-Null
}
function Write-AgentLog([string]$Message) {
  $line = "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
  Add-Content -LiteralPath $Log -Value $line -Encoding UTF8
}
function Get-BadailPython {
  foreach ($p in @(
      (Join-Path $env:LOCALAPPDATA "Python\pythoncore-3.14-64\python.exe"),
      (Join-Path $env:LOCALAPPDATA "Python\pythoncore-3.13-64\python.exe"),
      (Join-Path $env:LOCALAPPDATA "Python\pythoncore-3.12-64\python.exe"),
      (Join-Path $env:LOCALAPPDATA "Python\bin\python.exe")
    )) {
    if (Test-Path -LiteralPath $p) { return $p }
  }
  foreach ($name in @("py", "python")) {
    $c = Get-Command $name -ErrorAction SilentlyContinue
    if ($c -and $c.Source -and ($c.Source -notmatch "WindowsApps")) { return $c.Source }
  }
  throw "python not found"
}
if (-not (Test-Path -LiteralPath $Py)) { throw "Missing $Py" }
Set-Location -LiteralPath $Root
$python = Get-BadailPython
Write-AgentLog "starting $python $Py"
while ($true) {
  try {
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $python
    if ($python -match '\\py\.exe$') { $psi.Arguments = '-3 "' + $Py + '"' }
    else { $psi.Arguments = '"' + $Py + '"' }
    $psi.WorkingDirectory = $Root
    $psi.WindowStyle = [System.Diagnostics.ProcessWindowStyle]::Hidden
    $psi.CreateNoWindow = $true
    $psi.UseShellExecute = $false
    $proc = [System.Diagnostics.Process]::Start($psi)
    $proc.WaitForExit()
    Write-AgentLog ("exited code " + $proc.ExitCode)
  } catch {
    Write-AgentLog ("crash: " + $_.Exception.Message)
  }
  Start-Sleep -Seconds 2
}
