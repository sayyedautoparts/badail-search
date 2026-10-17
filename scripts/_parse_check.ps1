$path = Join-Path $PSScriptRoot "setup_upload_on_this_pc.ps1"
$tokens = $null
$errors = $null
[void][System.Management.Automation.Language.Parser]::ParseFile($path, [ref]$tokens, [ref]$errors)
if ($errors -and $errors.Count -gt 0) {
  $errors | ForEach-Object { Write-Host $_.ToString() }
  exit 1
}
Write-Host "PARSE_OK"
exit 0
