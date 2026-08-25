"""Run the StreamForge telemetry consumer."""

from backend.api.routes.telemetry import add_telemetry
from backend.kafka.consumer import TelemetryConsumer


def run() -> None:
    """Consume telemetry events continuously."""

    consumer = TelemetryConsumer(
        group_id="streamforge-telemetry-service"
    )

    print("Telemetry consumer started.")

    try:
        while True:
            telemetry = consumer.consume_one(timeout=1.0)

            if telemetry is None:
                continue

            # Store the real Kafka telemetry event
            # so the FastAPI dashboard endpoint can access it.
            add_telemetry(telemetry)

            print(
                f"Received telemetry: "
                f"truck={telemetry.truck_id}, "
                f"temperature={telemetry.temperature}, "
                f"timestamp={telemetry.timestamp}"
            )

    except KeyboardInterrupt:
        print("Stopping telemetry consumer.")

    finally:
        consumer.close()


if __name__ == "__main__":
    run()