#!/usr/bin/env bash
# ==============================================================================
# GITHUB REPOSITORY LINKING AND PUBLISHING HELPER (BASH)
# ==============================================================================

set -euo pipefail

if [ "$#" -lt 1 ]; then
    echo "Usage: $0 <github_username> [repo_name]"
    exit 1
fi

GITHUB_USERNAME="$1"
REPO_NAME="${2:-flight-weather-analytics-engine}"
REMOTE_URL="https://github.com/$GITHUB_USERNAME/$REPO_NAME.git"

echo "=== Setting up Git Remote for GitHub ==="
echo "Target Remote: $REMOTE_URL"

git branch -M main

if git remote get-url origin &>/dev/null; then
    echo "Updating existing origin remote URL..."
    git remote set-url origin "$REMOTE_URL"
else
    echo "Adding origin remote..."
    git remote add origin "$REMOTE_URL"
fi

echo "Pushing main branch to GitHub..."
git push -u origin main

echo "✨ Successfully published $REPO_NAME to GitHub!"
echo "Repository URL: https://github.com/$GITHUB_USERNAME/$REPO_NAME"
