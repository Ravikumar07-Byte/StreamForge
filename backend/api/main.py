"""StreamForge FastAPI application."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.routes.health import router as health_router
from backend.state.snapshot import load_snapshot

app = FastAPI(
    title="StreamForge API",
    version="1.0.0",
    description="Real-time truck telemetry streaming API",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router, prefix="/api")


@app.get("/api/telemetry")
def telemetry() -> dict:
    """Return the latest dashboard telemetry and active alerts."""

    snapshot = load_snapshot()

    return {
        "kafka_status": snapshot.get("kafka_status", "Online"),
        "telemetry": snapshot.get("telemetry", []),
        "alerts": snapshot.get("alerts", []),
    }


@app.get("/api/metrics")
def metrics() -> dict:
    """Return the latest persistent dashboard metrics."""

    snapshot = load_snapshot()

    return snapshot.get(
        "metrics",
        {
            "events_received": 0,
            "events_processed": 0,
            "events_invalid": 0,
            "events_late": 0,
            "active_trucks": 0,
        },
    )


@app.get("/")
def root() -> dict[str, str]:
    """Return API information."""

    return {
        "service": "StreamForge API",
        "status": "running",
        "version": "1.0.0",
    }