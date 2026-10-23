#Requires -Version 5.1
$ErrorActionPreference = "SilentlyContinue"
$TaskName = "BadailRongtaPrintAgent"
Stop-ScheduledTask -TaskName $TaskName
Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
Remove-ItemProperty -Path "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run" -Name $TaskName -ErrorAction SilentlyContinue
Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='pythonw.exe' OR Name='powershell.exe'" |
  Where-Object { $_.CommandLine -and ($_.CommandLine -match 'rongta_local_print\.py' -or $_.CommandLine -match 'start_rongta_print_agent\.ps1') } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Write-Host "Removed print agent from this PC. Print jobs will not come here."
