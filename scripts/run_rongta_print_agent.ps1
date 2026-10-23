#Requires -Version 5.1
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Py = Join-Path $PSScriptRoot "rongta_local_print.py"
$python = (Get-Command python -ErrorAction SilentlyContinue)
if (-not $python) { throw "python not found" }
Set-Location -LiteralPath $Root
& $python.Source $Py
