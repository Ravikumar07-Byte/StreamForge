"""StreamForge FastAPI application."""

import threading

from fastapi import FastAPI

from backend.api.routes.health import router as health_router
from backend.api.routes.telemetry import add_telemetry, get_telemetry
from backend.kafka.consumer import TelemetryConsumer


app = FastAPI(
    title="StreamForge API",
    version="1.0.0",
)


app.include_router(health_router, prefix="/api")


def consume_telemetry() -> None:
    """Continuously consume telemetry from Kafka."""

    consumer = TelemetryConsumer(
        group_id="streamforge-api-consumer",
        auto_offset_reset="latest",
    )

    print("StreamForge API Kafka consumer started.")

    try:
        while True:
            telemetry = consumer.consume_one(timeout=1.0)

            if telemetry is None:
                continue

            add_telemetry(telemetry)

            print(
                f"API received telemetry: "
                f"truck={telemetry.truck_id}, "
                f"temperature={telemetry.temperature}, "
                f"timestamp={telemetry.timestamp}"
            )

    except Exception as exc:
        print(f"Telemetry consumer stopped: {exc}")

    finally:
        consumer.close()


@app.on_event("startup")
def start_telemetry_consumer() -> None:
    """Start the Kafka consumer in a background thread."""

    thread = threading.Thread(
        target=consume_telemetry,
        daemon=True,
    )

    thread.start()


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