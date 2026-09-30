# ============================================================
# StreamForge Demo Launcher
# Week 1 + Week 2 Review
# ============================================================

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $ProjectRoot

Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "              STREAMFORGE DEMO STARTUP" -ForegroundColor Cyan
Write-Host "             Week 1 + Week 2 Review" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# ------------------------------------------------------------
# 1. Check virtual environment
# ------------------------------------------------------------

if (-not (Test-Path "$ProjectRoot\.venv\Scripts\python.exe")) {
    Write-Host "[ERROR] .venv was not found." -ForegroundColor Red
    Write-Host "Create the virtual environment first." -ForegroundColor Yellow
    pause
    exit
}

$Python = "$ProjectRoot\.venv\Scripts\python.exe"

Write-Host "[1/5] Starting Kafka..." -ForegroundColor Yellow

# ------------------------------------------------------------
# 2. Start Kafka
# ------------------------------------------------------------

docker compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] Docker Compose failed." -ForegroundColor Red
    pause
    exit
}

Write-Host "[OK] Kafka/Docker started." -ForegroundColor Green

Start-Sleep -Seconds 5

# ------------------------------------------------------------
# 3. Start Bytewax
# ------------------------------------------------------------

Write-Host "[2/5] Starting Bytewax stream processor..." -ForegroundColor Yellow

Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-Command",
    "Set-Location '$ProjectRoot'; & '$Python' -m bytewax.run backend.streaming.bytewax_pipeline"
)

Write-Host "[OK] Bytewax window opened." -ForegroundColor Green

Start-Sleep -Seconds 2

# ------------------------------------------------------------
# 4. Start telemetry producer
# ------------------------------------------------------------

Write-Host "[3/5] Starting telemetry producer..." -ForegroundColor Yellow

Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-Command",
    "Set-Location '$ProjectRoot'; & '$Python' -m backend.producers.live_telemetry"
)

Write-Host "[OK] Telemetry producer window opened." -ForegroundColor Green

Start-Sleep -Seconds 2

# ------------------------------------------------------------
# 5. Start FastAPI
# ------------------------------------------------------------

Write-Host "[4/5] Starting FastAPI backend..." -ForegroundColor Yellow

Start-Process powershell -ArgumentList @(
    "-NoExit",
    "-Command",
    "Set-Location '$ProjectRoot'; & '$Python' -m uvicorn backend.api.main:app --reload --host 127.0.0.1 --port 8000"
)

Write-Host "[OK] FastAPI window opened." -ForegroundColor Green

Start-Sleep -Seconds 3

# ------------------------------------------------------------
# 6. Start React/Vite
# ------------------------------------------------------------

Write-Host "[5/5] Starting React dashboard..." -ForegroundColor Yellow

if (-not (Test-Path "$ProjectRoot\frontend\package.json")) {
    Write-Host "[WARNING] frontend/package.json was not found." -ForegroundColor Yellow
}
else {
    Start-Process powershell -ArgumentList @(
        "-NoExit",
        "-Command",
        "Set-Location '$ProjectRoot\frontend'; npm run dev"
    )

    Write-Host "[OK] React dashboard window opened." -ForegroundColor Green
}

# ------------------------------------------------------------
# Final message
# ------------------------------------------------------------

Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "              STREAMFORGE IS STARTING" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Kafka       : http://localhost:9092" -ForegroundColor White
Write-Host "FastAPI     : http://127.0.0.1:8000" -ForegroundColor White
Write-Host "API Docs    : http://127.0.0.1:8000/docs" -ForegroundColor White
Write-Host ""
Write-Host "Open the Vite URL shown in the frontend terminal." -ForegroundColor Cyan
Write-Host ""
Write-Host "Week 1 : Kafka + Telemetry" -ForegroundColor White
Write-Host "Week 2 : Bytewax + Filtering + Windows + Late Events" -ForegroundColor White
Write-Host "Week 3 : RocksDB + Recovery       [Later]" -ForegroundColor DarkGray
Write-Host "Week 4 : Prometheus + Dashboard    [Later]" -ForegroundColor DarkGray
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
