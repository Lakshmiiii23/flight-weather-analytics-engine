# ==============================================================================
# WINDOWS 1-CLICK RUNNER FOR FLIGHT-WEATHER ENGINE
# Runs locally on Windows without requiring Linux / sudo / GCP VM
# ==============================================================================

param (
    [ValidateSet("dashboard", "pipeline-dry-run", "pipeline-live", "test")]
    [string]$Mode = "dashboard"
)

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "🛫 ECO-FRIENDLY FLIGHT-WEATHER ENGINE (WINDOWS LAUNCHER)" -ForegroundColor Yellow
Write-Host "==========================================================" -ForegroundColor Cyan

switch ($Mode) {
    "dashboard" {
        Write-Host "Launching 3D Streamlit & PyDeck Web Dashboard..." -ForegroundColor Green
        Write-Host "URL: http://localhost:8501" -ForegroundColor Cyan
        streamlit run src/dashboard/app.py
    }
    "pipeline-dry-run" {
        Write-Host "Running 1 cycle of streaming ingestion in Dry-Run mode..." -ForegroundColor Green
        python scripts/run_pipeline.py --dry-run --once
    }
    "pipeline-live" {
        Write-Host "Running continuous micro-batch streaming (every 10 minutes)..." -ForegroundColor Green
        python scripts/run_pipeline.py --interval 600
    }
    "test" {
        Write-Host "Running full 56-test automated test suite..." -ForegroundColor Green
        python -m pytest tests/ -v
    }
}
