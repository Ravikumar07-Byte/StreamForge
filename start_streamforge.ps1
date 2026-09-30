# ============================================================
# StreamForge - Complete Startup Script
# ============================================================

$Root = "C:\Users\rr410\StreamForge"
$Python = "$Root\.venv\Scripts\python.exe"
$Activate = "$Root\.venv\Scripts\Activate.ps1"
$Frontend = "$Root\frontend"

Write-Host ""
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "       STREAMFORGE STARTUP" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

# ------------------------------------------------------------
# 0. VERIFY VIRTUAL ENVIRONMENT
# ------------------------------------------------------------

Write-Host "[0/6] Checking Python environment..." -ForegroundColor Yellow

if (!(Test-Path $Python)) {
    Write-Host "ERROR: .venv Python not found!" -ForegroundColor Red
    exit 1
}

Write-Host "Python: $Python" -ForegroundColor Green


# ------------------------------------------------------------
# 1. START KAFKA
# ------------------------------------------------------------

Write-Host ""
Write-Host "[1/6] Starting Kafka..." -ForegroundColor Yellow

Set-Location $Root

docker compose up -d

Start-Sleep -Seconds 5

$Kafka = docker ps --filter "name=streamforge-kafka" --format "{{.Status}}"

if ($Kafka) {
    Write-Host "Kafka: ONLINE" -ForegroundColor Green
}
else {
    Write-Host "Kafka: NOT RUNNING" -ForegroundColor Red
}


# ------------------------------------------------------------
# 2. START FASTAPI
# ------------------------------------------------------------

Write-Host ""
Write-Host "[2/6] Starting FastAPI..." -ForegroundColor Yellow

Start-Process powershell.exe `
    -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command",
    "& {
        Set-Location '$Root'
        & '$Activate'
        Write-Host ''
        Write-Host '=== STREAMFORGE FASTAPI ===' -ForegroundColor Cyan
        Write-Host 'Python:' (Get-Command python).Source -ForegroundColor Green
        python -m uvicorn backend.api.main:app --host 127.0.0.1 --port 8000 --reload
    }"

Start-Sleep -Seconds 5

Write-Host "FastAPI: STARTING" -ForegroundColor Green


# ------------------------------------------------------------
# 3. START TELEMETRY CONSUMER
# ------------------------------------------------------------

Write-Host ""
Write-Host "[3/6] Starting telemetry consumer..." -ForegroundColor Yellow

Start-Process powershell.exe `
    -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command",
    "& {
        Set-Location '$Root'
        & '$Activate'
        Write-Host ''
        Write-Host '=== STREAMFORGE TELEMETRY CONSUMER ===' -ForegroundColor Cyan
        Write-Host 'Python:' (Get-Command python).Source -ForegroundColor Green
        python -m backend.consumers.telemetry_consumer
    }"

Start-Sleep -Seconds 3

Write-Host "Consumer: STARTING" -ForegroundColor Green


# ------------------------------------------------------------
# 4. START LIVE TELEMETRY PRODUCER
# ------------------------------------------------------------

Write-Host ""
Write-Host "[4/6] Starting live telemetry producer..." -ForegroundColor Yellow

Start-Process powershell.exe `
    -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-Command",
    "& {
        Set-Location '$Root'
        & '$Activate'
        Write-Host ''
        Write-Host '=== STREAMFORGE LIVE TELEMETRY PRODUCER ===' -ForegroundColor Cyan
        Write-Host 'Python:' (Get-Command python).Source -ForegroundColor Green
        python -m backend.producers.live_telemetry
    }"

Start-Sleep -Seconds 3

Write-Host "Producer: STARTING" -ForegroundColor Green


# ------------------------------------------------------------
# 5. START REACT FRONTEND
# ------------------------------------------------------------

Write-Host ""
Write-Host "[5/6] Starting React frontend..." -ForegroundColor Yellow

Start-Process powershell.exe `
    -ArgumentList "-NoExit", "-Command",
    "& {
        Set-Location '$Frontend'
        Write-Host ''
        Write-Host '=== STREAMFORGE FRONTEND ===' -ForegroundColor Cyan
        npm run dev
    }"

Start-Sleep -Seconds 8

Write-Host "Frontend: STARTING" -ForegroundColor Green


# ------------------------------------------------------------
# 6. OPEN BROWSER
# ------------------------------------------------------------

Write-Host ""
Write-Host "[6/6] Opening StreamForge..." -ForegroundColor Yellow

Start-Process "http://localhost:5173"

Start-Sleep -Seconds 2

Start-Process "http://127.0.0.1:8000/docs"


# ------------------------------------------------------------
# FINAL STATUS
# ------------------------------------------------------------

Write-Host ""
Write-Host "=========================================" -ForegroundColor Green
Write-Host "       STREAMFORGE IS READY" -ForegroundColor Green
Write-Host "=========================================" -ForegroundColor Green
Write-Host ""

Write-Host "Frontend : http://localhost:5173" -ForegroundColor White
Write-Host "Swagger  : http://127.0.0.1:8000/docs" -ForegroundColor White
Write-Host "API      : http://127.0.0.1:8000" -ForegroundColor White
Write-Host "Kafka    : localhost:9092" -ForegroundColor White

Write-Host ""
Write-Host "Python environment:" -ForegroundColor Cyan
Write-Host $Python -ForegroundColor Green

Write-Host ""
Write-Host "Keep the opened terminals running." -ForegroundColor Yellow
Write-Host ""