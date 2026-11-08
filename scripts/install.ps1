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
#   https://cdn.tabmail.ai/releases/windows-x86_64/update-manifest.json
#   https://cdn.tabmail.ai/releases/windows-x86_64/fts_helper-<version>.exe
#   https://cdn.tabmail.ai/releases/windows-x86_64/fts_helper-<version>-THIRD_PARTY_NOTICES.txt
# The binary and its notices are taken from the release the update manifest names. A release
# publishes the manifest last, so both files of that version are on the CDN; the two "-latest"
# copies are replaced one at a time and can briefly belong to different releases.
$Cdn = "https://cdn.tabmail.ai/releases/windows-x86_64"
$HelperPath = "$InstallDir\fts_helper.exe"
$NoticesPath = "$InstallDir\fts_helper-THIRD_PARTY_NOTICES.txt"
$HelperDownload = "$InstallDir\fts_helper.exe.download"
$NoticesDownload = "$InstallDir\fts_helper-THIRD_PARTY_NOTICES.txt.download"
$ManifestPath = "$InstallDir\tabmail_fts.json"
$RegistryPath = "HKCU:\Software\Mozilla\NativeMessagingHosts\tabmail_fts"

Write-Host "Installing to: $InstallDir"
Write-Host ""

# Create directory
Write-Host "Creating directory..."
New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null

# Download the binary and its notices next to the install, then put both in place: a failed
# download leaves an earlier install as it was. The binary goes first: Windows refuses to replace a
# running helper's file, and that leaves the earlier pair. Unlike a self-update, the pair is not
# kept when the notices then fail: the installer reports it, and the new binary puts its own notices
# there on its next hello once the file can be replaced.
try {
    Write-Host "Reading the current release..."
    $UpdateManifest = Invoke-RestMethod -Uri "$Cdn/update-manifest.json"
    $Version = [string]$UpdateManifest.latest.version
    if ($Version -notmatch '^[0-9]+\.[0-9]+\.[0-9]+\z') {
        throw "The update manifest names no release version."
    }
    Write-Host "Downloading native-fts $Version and its third-party notices..."
    Invoke-WebRequest -Uri "$Cdn/fts_helper-$Version.exe" -OutFile $HelperDownload -UseBasicParsing
    Invoke-WebRequest -Uri "$Cdn/fts_helper-$Version-THIRD_PARTY_NOTICES.txt" -OutFile $NoticesDownload -UseBasicParsing
    Move-Item -Force -LiteralPath $HelperDownload -Destination $HelperPath
    Move-Item -Force -LiteralPath $NoticesDownload -Destination $NoticesPath
} catch {
    Write-Host "Error: Failed to install native-fts" -ForegroundColor Red
    Write-Host $_.Exception.Message
    Write-Host "If Thunderbird is running, close it and run the installer again."
    Remove-Item -Force -ErrorAction SilentlyContinue -LiteralPath $HelperDownload, $NoticesDownload
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
Write-Host "  Helper:   $HelperPath ($Version)"
Write-Host "  Notices:  $NoticesPath"
Write-Host "  Manifest: $ManifestPath"
Write-Host "  Registry: $RegistryPath"
Write-Host ""
Write-Host "Please restart Thunderbird for changes to take effect." -ForegroundColor Yellow
