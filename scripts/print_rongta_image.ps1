#Requires -Version 5.1
param(
  [Parameter(Mandatory = $true)][string]$ImagePath,
  [string]$PrinterName = "Rongta RP4xx Series",
  [int]$Copies = 1,
  [double]$WidthMm = 105,
  [double]$HeightMm = 28
)
$ErrorActionPreference = "Stop"
Add-Type -AssemblyName System.Drawing

if (-not (Test-Path -LiteralPath $ImagePath)) { throw "Missing image: $ImagePath" }
$Copies = [Math]::Max(1, $Copies)

$bmp = [System.Drawing.Bitmap]::FromFile($ImagePath)
try {
  $doc = New-Object System.Drawing.Printing.PrintDocument
  $doc.PrinterSettings.PrinterName = $PrinterName
  if (-not $doc.PrinterSettings.IsValid) { throw "Printer not ready: $PrinterName" }
  $doc.DocumentName = "Badail-105x28"
  $doc.OriginAtMargins = $false
  $doc.PrinterSettings.Copies = [int16]$Copies
  $doc.DefaultPageSettings.Color = $false
  $doc.DefaultPageSettings.Landscape = $false
  $doc.DefaultPageSettings.Margins = New-Object System.Drawing.Printing.Margins 0, 0, 0, 0

  $chosen = $null
  foreach ($p in $doc.PrinterSettings.PaperSizes) {
    $n = [string]$p.PaperName
    if ($n -match '105' -and $n -match '28') { $chosen = $p; break }
    if ($n -match 'Badail') { $chosen = $p; break }
  }
  if (-not $chosen) {
    $w = [int][Math]::Round($WidthMm / 25.4 * 100)
    $h = [int][Math]::Round($HeightMm / 25.4 * 100)
    $chosen = New-Object System.Drawing.Printing.PaperSize "Badail 105x28", $w, $h
    $chosen.RawKind = 256
  }
  $doc.DefaultPageSettings.PaperSize = $chosen

  foreach ($r in $doc.PrinterSettings.PrinterResolutions) {
    if ($r.X -eq 203 -or $r.Y -eq 203) {
      $doc.DefaultPageSettings.PrinterResolution = $r
      break
    }
  }

  $script:printBmp = $bmp
  $script:printW = $WidthMm
  $script:printH = $HeightMm
  $doc.add_PrintPage({
    param($sender, $e)
    $g = $e.Graphics
    $g.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::NearestNeighbor
    $g.PixelOffsetMode = [System.Drawing.Drawing2D.PixelOffsetMode]::Half
    $g.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::None
    $g.CompositingQuality = [System.Drawing.Drawing2D.CompositingQuality]::HighSpeed
    $g.PageUnit = [System.Drawing.GraphicsUnit]::Millimeter
    $g.DrawImage($script:printBmp, 0.0, 0.0, $script:printW, $script:printH)
    $e.HasMorePages = $false
  })
  $doc.Print()
}
finally {
  $bmp.Dispose()
}
Write-Host "printed"
