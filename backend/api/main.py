"""StreamForge FastAPI application."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.routes.health import router as health_router
from backend.api.routes.telemetry import get_persisted_truck_states
from backend.metrics.prometheus import (
    active_trucks,
    telemetry_events_invalid,
    telemetry_events_late,
    telemetry_events_processed,
    telemetry_events_received,
)
from backend.state.metrics_state import load_metrics
from backend.state.rocksdb_store import RocksDBStore


STATE_PATH = "data/state"


app = FastAPI(
    title="StreamForge API",
    version="1.0.0",
    description="Real-time truck telemetry streaming API",
)


# ---------------------------------------------------------
# CORS
# ---------------------------------------------------------

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


# ---------------------------------------------------------
# Routes
# ---------------------------------------------------------

app.include_router(
    health_router,
    prefix="/api",
)


# ---------------------------------------------------------
# Telemetry API
# ---------------------------------------------------------

@app.get("/api/telemetry")
def telemetry() -> dict:
    """Return latest persisted truck telemetry."""

    states = get_persisted_truck_states()

    return {
        "kafka_status": "Online",
        "telemetry": [
            {
                "truck": state["truck_id"],
                "temperature": state["temperature"],
                "timestamp": state["timestamp"],
            }
            for state in states
        ],
    }


# ---------------------------------------------------------
# Metrics API
# ---------------------------------------------------------

@app.get("/api/metrics")
def metrics() -> dict[str, int | float]:
    """Return persistent StreamForge processing metrics."""

    store = RocksDBStore(STATE_PATH)

    try:
        return load_metrics(store)

    finally:
        store.close()


# ---------------------------------------------------------
# Root endpoint
# ---------------------------------------------------------

@app.get("/")
def root() -> dict[str, str]:
    """Return API information."""

    return {
        "service": "StreamForge API",
        "status": "running",
        "version": "1.0.0",
    }