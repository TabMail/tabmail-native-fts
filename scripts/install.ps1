# TabMail Native FTS Helper - Install Script (Windows)
# Installs the Rust native host binary to user directory (no admin required)
#
# Usage:
#   irm https://raw.githubusercontent.com/TabMail/tabmail-native-fts/main/scripts/install.ps1 | iex

$ErrorActionPreference = "Stop"

Write-Host "TabMail Native FTS Helper - Installer" -ForegroundColor Blue
Write-Host ""

# Set paths
$InstallDir = "$env:LOCALAPPDATA\TabMail\native"
# Platform-first CDN layout (independent per platform):
#   https://cdn.tabmail.ai/releases/windows-x86_64/fts_helper-latest.exe
$HelperUrl = "https://cdn.tabmail.ai/releases/windows-x86_64/fts_helper-latest.exe"
$HelperPath = "$InstallDir\fts_helper.exe"
$ManifestPath = "$InstallDir\tabmail_fts.json"
$RegistryPath = "HKCU:\Software\Mozilla\NativeMessagingHosts\tabmail_fts"

Write-Host "Installing to: $InstallDir"
Write-Host ""

# Create directory
Write-Host "Creating directory..."
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null

# Download binary
Write-Host "Downloading native-fts binary..."
try {
    Invoke-WebRequest -Uri $HelperUrl -OutFile $HelperPath -UseBasicParsing
} catch {
    Write-Host "Error: Failed to download native-fts binary" -ForegroundColor Red
    Write-Host $_.Exception.Message
    exit 1
}

# Create native messaging manifest
Write-Host "Creating native messaging manifest..."
# ConvertTo-Json automatically escapes backslashes in paths
$Manifest = @{
    name = "tabmail_fts"
    description = "TabMail FTS Native Helper"
    path = $HelperPath
    type = "stdio"
    allowed_extensions = @("thunderbird@tabmail.ai")
} | ConvertTo-Json -Depth 10

# Write without BOM (PowerShell 5.x's -Encoding UTF8 adds BOM which breaks JSON parsing)
[System.IO.File]::WriteAllText($ManifestPath, $Manifest)

# Create registry key pointing to manifest
Write-Host "Setting up registry..."
New-Item -Path $RegistryPath -Force | Out-Null
Set-ItemProperty -Path $RegistryPath -Name "(Default)" -Value $ManifestPath

Write-Host ""
Write-Host "Installation complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Installed to:"
Write-Host "  Helper:   $HelperPath"
Write-Host "  Manifest: $ManifestPath"
Write-Host "  Registry: $RegistryPath"
Write-Host ""
Write-Host "Please restart Thunderbird for changes to take effect." -ForegroundColor Yellow
