# ==============================================================================
# GITHUB REPOSITORY LINKING AND PUBLISHING HELPER (POWERSHELL)
# ==============================================================================

param (
    [Parameter(Mandatory=$true)]
    [string]$GitHubUsername,
    
    [string]$RepoName = "flight-weather-analytics-engine"
)

$RemoteUrl = "https://github.com/$GitHubUsername/$RepoName.git"

Write-Host "=== Setting up Git Remote for GitHub ===" -ForegroundColor Cyan
Write-Host "Target Remote: $RemoteUrl" -ForegroundColor Yellow

# Ensure main branch
git branch -M main

# Check if origin remote already exists
$ExistingRemote = git remote get-url origin 2>$null
if ($ExistingRemote) {
    Write-Host "Updating existing origin remote URL..." -ForegroundColor Gray
    git remote set-url origin $RemoteUrl
} else {
    Write-Host "Adding origin remote..." -ForegroundColor Gray
    git remote add origin $RemoteUrl
}

Write-Host "Pushing main branch to GitHub..." -ForegroundColor Cyan
git push -u origin main

Write-Host "✨ Successfully published $RepoName to GitHub!" -ForegroundColor Green
Write-Host "Repository URL: https://github.com/$GitHubUsername/$RepoName" -ForegroundColor Green
