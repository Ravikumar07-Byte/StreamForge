# ============================================================
# StreamForge - Complete Startup Script
# ============================================================

$Root = "C:\Users\rr410\StreamForge"
$Python = "$Root\.venv\Scripts\python.exe"
$Frontend = "$Root\frontend"
$PrometheusDir = "$Root\data\prometheus"

Write-Host ""
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "         STREAMFORGE STARTUP" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""

# ------------------------------------------------------------
# Project root
# ------------------------------------------------------------

Set-Location $Root

# ------------------------------------------------------------
# Prometheus multiprocess configuration
#
# IMPORTANT:
# This is configured BEFORE any Python worker starts.
# ------------------------------------------------------------

New-Item -ItemType Directory -Force $PrometheusDir | Out-Null

# Remove only old Prometheus multiprocess files.
# DO NOT touch data/state because that contains RocksDB state.
Get-ChildItem $PrometheusDir -Force -ErrorAction SilentlyContinue |
    Remove-Item -Recurse -Force

$env:PROMETHEUS_MULTIPROC_DIR = $PrometheusDir

Write-Host "Prometheus multiprocess directory:" -ForegroundColor Yellow
Write-Host "  $env:PROMETHEUS_MULTIPROC_DIR" -ForegroundColor Green
Write-Host ""

# ------------------------------------------------------------
# Start Kafka
# ------------------------------------------------------------

Write-Host "[1/6] Starting Kafka..." -ForegroundColor Cyan

docker compose up -d

Start-Sleep -Seconds 5

# ------------------------------------------------------------
# Start telemetry consumer
# ------------------------------------------------------------

Write-Host "[2/6] Starting telemetry consumer..." -ForegroundColor Cyan

Start-Process powershell.exe `
    -ArgumentList `
        "-NoExit",
        "-Command",
        "Set-Location '$Root'; & '$Python' -m backend.consumers.telemetry_consumer"

Start-Sleep -Seconds 3

# ------------------------------------------------------------
# Start live telemetry producer
# ------------------------------------------------------------

Write-Host "[3/6] Starting live telemetry producer..." -ForegroundColor Cyan

Start-Process powershell.exe `
    -ArgumentList `
        "-NoExit",
        "-Command",
        "Set-Location '$Root'; & '$Python' -m backend.producers.live_telemetry"

Start-Sleep -Seconds 3

# ------------------------------------------------------------
# Start FastAPI
# ------------------------------------------------------------

Write-Host "[4/6] Starting FastAPI..." -ForegroundColor Cyan

Start-Process powershell.exe `
    -ArgumentList `
        "-NoExit",
        "-Command",
        "Set-Location '$Root'; & '$Python' -m uvicorn backend.api.main:app --host 127.0.0.1 --port 8000"

Start-Sleep -Seconds 5

# ------------------------------------------------------------
# Start React frontend
# ------------------------------------------------------------

Write-Host "[5/6] Starting React frontend..." -ForegroundColor Cyan

Start-Process powershell.exe `
    -ArgumentList `
        "-NoExit",
        "-Command",
        "Set-Location '$Frontend'; npm run dev"

Start-Sleep -Seconds 8

# ------------------------------------------------------------
# Open application pages
# ------------------------------------------------------------

Write-Host "[6/6] Opening StreamForge..." -ForegroundColor Cyan

Start-Process "http://localhost:5173"

Start-Sleep -Seconds 2

Start-Process "http://127.0.0.1:8000/docs"

Write-Host ""
Write-Host "==================================================" -ForegroundColor Green
Write-Host "       STREAMFORGE STARTUP COMPLETE" -ForegroundColor Green
Write-Host "==================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Frontend : http://localhost:5173" -ForegroundColor White
Write-Host "API      : http://127.0.0.1:8000" -ForegroundColor White
Write-Host "Swagger  : http://127.0.0.1:8000/docs" -ForegroundColor White
Write-Host "Metrics  : http://127.0.0.1:8000/metrics" -ForegroundColor White
Write-Host ""
Write-Host "Prometheus multiprocess directory:" -ForegroundColor Yellow
Write-Host "$env:PROMETHEUS_MULTIPROC_DIR" -ForegroundColor Green
Write-Host ""
Write-Host "Kafka, Consumer, Producer, FastAPI and React are starting." -ForegroundColor Cyan
Write-Host ""
