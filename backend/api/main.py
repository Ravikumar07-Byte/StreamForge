"""StreamForge FastAPI application."""

import threading

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.routes.health import router as health_router
from backend.api.routes.telemetry import add_telemetry, get_telemetry
from backend.kafka.consumer import TelemetryConsumer
from backend.metrics.prometheus import (
    active_trucks,
    record_invalid,
    record_processed,
    record_received,
    telemetry_events_invalid,
    telemetry_events_late,
    telemetry_events_processed,
    telemetry_events_received,
    set_active_trucks,
)
from backend.streaming.dataflow import process_telemetry


# ---------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------

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
# Kafka Consumer
# ---------------------------------------------------------

def consume_telemetry() -> None:
    """Continuously consume and process telemetry from Kafka."""

    consumer = TelemetryConsumer(
        group_id="streamforge-api-consumer",
        auto_offset_reset="latest",
    )

    # Keep track of trucks that have successfully
    # reported telemetry during this API session.
    active_truck_ids: set[str] = set()

    print("StreamForge API Kafka consumer started.")

    try:
        while True:
            try:
                telemetry = consumer.consume_one(
                    timeout=1.0
                )

                if telemetry is None:
                    continue

                # -------------------------------------------------
                # Event received from Kafka
                # -------------------------------------------------

                record_received()

                # -------------------------------------------------
                # Process telemetry through streaming pipeline
                # -------------------------------------------------

                processed = process_telemetry(
                    telemetry
                )

                # -------------------------------------------------
                # Reject invalid telemetry
                # -------------------------------------------------

                if processed is None:
                    record_invalid()

                    print(
                        "Invalid telemetry rejected: "
                        f"truck={telemetry.truck_id}, "
                        f"temperature={telemetry.temperature}"
                    )

                    continue

                # -------------------------------------------------
                # Successful processing
                # -------------------------------------------------

                record_processed()

                # Track active trucks
                active_truck_ids.add(
                    processed.truck_id
                )

                set_active_trucks(
                    len(active_truck_ids)
                )

                # -------------------------------------------------
                # Store processed event for dashboard
                # -------------------------------------------------

                add_telemetry(processed)

                # -------------------------------------------------
                # Commit only after successful processing
                # -------------------------------------------------

                consumer.commit()

                print(
                    "API processed telemetry: "
                    f"truck={processed.truck_id}, "
                    f"temperature={processed.temperature}, "
                    f"timestamp={processed.timestamp}"
                )

            except Exception as exc:
                print(
                    f"Telemetry processing error: {exc}"
                )

    except Exception as exc:
        print(
            f"Telemetry consumer stopped: {exc}"
        )

    finally:
        consumer.close()


# ---------------------------------------------------------
# Start Kafka consumer when API starts
# ---------------------------------------------------------

@app.on_event("startup")
def start_telemetry_consumer() -> None:
    """Start Kafka consumer in a background thread."""

    thread = threading.Thread(
        target=consume_telemetry,
        daemon=True,
        name="streamforge-kafka-consumer",
    )

    thread.start()


# ---------------------------------------------------------
# Telemetry endpoint
# ---------------------------------------------------------

@app.get("/api/telemetry")
def telemetry() -> dict:
    """Return recent processed truck telemetry."""

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


# ---------------------------------------------------------
# Metrics endpoint
# ---------------------------------------------------------

@app.get("/api/metrics")
def metrics() -> dict[str, int | float]:
    """Return StreamForge processing metrics."""

    return {
        "events_received": int(
            telemetry_events_received._value.get()
        ),
        "events_processed": int(
            telemetry_events_processed._value.get()
        ),
        "events_invalid": int(
            telemetry_events_invalid._value.get()
        ),
        "events_late": int(
            telemetry_events_late._value.get()
        ),
        "active_trucks": int(
            active_trucks._value.get()
        ),
    }


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