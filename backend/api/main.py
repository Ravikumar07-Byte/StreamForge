"""StreamForge FastAPI application."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import (
    CollectorRegistry,
    make_asgi_app,
    multiprocess,
)

from backend.api.routes.health import router as health_router
from backend.state.snapshot import load_snapshot

# Import StreamForge metrics so the metric definitions are
# available in the application environment.
from backend.metrics import prometheus


app = FastAPI(
    title="StreamForge API",
    version="1.0.0",
    description="Real-time truck telemetry streaming API",
)


# ============================================================
# PROMETHEUS MULTIPROCESS METRICS
# ============================================================

def create_prometheus_app():
    """Create a Prometheus endpoint aggregating all worker processes."""

    registry = CollectorRegistry()

    multiprocess.MultiProcessCollector(
        registry,
    )

    return make_asgi_app(
        registry=registry,
    )


prometheus_app = create_prometheus_app()

app.mount(
    "/metrics",
    prometheus_app,
)


# ============================================================
# CORS
# ============================================================

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


# ============================================================
# ROUTES
# ============================================================

app.include_router(
    health_router,
    prefix="/api",
)


# ============================================================
# TELEMETRY API
# ============================================================

@app.get("/api/telemetry")
def telemetry() -> dict:
    """Return the latest dashboard telemetry snapshot."""

    snapshot = load_snapshot()

    return {
        "kafka_status": snapshot.get(
            "kafka_status",
            "Online",
        ),
        "telemetry": snapshot.get(
            "telemetry",
            [],
        ),
        "alerts": snapshot.get(
            "alerts",
            [],
        ),
    }


# ============================================================
# DASHBOARD METRICS API
# ============================================================

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


# ============================================================
# ROOT
# ============================================================

@app.get("/")
def root() -> dict[str, str]:
    """Return API information."""

    return {
        "service": "StreamForge API",
        "status": "running",
        "version": "1.0.0",
    }