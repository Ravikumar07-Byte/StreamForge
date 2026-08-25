"""StreamForge FastAPI application."""

from fastapi import FastAPI

from backend.api.routes.health import router as health_router
from backend.api.routes.telemetry import get_telemetry

app = FastAPI(
    title="StreamForge API",
    version="1.0.0",
)


app.include_router(health_router, prefix="/api")


@app.get("/api/telemetry")
def telemetry() -> dict:
    """Return recent truck telemetry."""
    events = get_telemetry()

    return {
        "kafka_status": "Online",
        "telemetry": [
            {
                "truck": event.truck_id,
                "temperature": event.temperature,
                "timestamp": event.timestamp.isoformat(),
            }
            for event in events
        ],
    }


@app.get("/")
def root() -> dict[str, str]:
    """Return API information."""
    return {
        "service": "StreamForge API",
        "status": "running",
    }