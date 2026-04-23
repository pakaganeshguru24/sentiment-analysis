param(
    [string]$ProjectRoot = $PSScriptRoot
)

$ErrorActionPreference = "Stop"

Set-Location $ProjectRoot
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
. "$ProjectRoot\venv\Scripts\Activate.ps1"

Write-Host "Activated virtual environment: $env:VIRTUAL_ENV_PROMPT" -ForegroundColor Green
Write-Host "Python: $((Get-Command python).Source)" -ForegroundColor DarkGreen