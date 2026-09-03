"""Continuously publish live synthetic truck telemetry to Kafka."""

import time

from backend.kafka.producer import TelemetryProducer
from backend.producers.telemetry_generator import (
    generate_telemetry,
    generate_truck_ids,
)


DEFAULT_TRUCK_COUNT = 5
DEFAULT_INTERVAL_SECONDS = 2.0


def run_live_telemetry(
    truck_count: int = DEFAULT_TRUCK_COUNT,
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
) -> None:
    """Continuously generate and publish telemetry for multiple trucks."""

    producer = TelemetryProducer()
    truck_ids = generate_truck_ids(truck_count)

    print("Live telemetry producer started.")
    print(f"Trucks: {', '.join(truck_ids)}")
    print(f"Interval: {interval_seconds} seconds")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            for truck_id in truck_ids:
                telemetry = generate_telemetry(truck_id)
                producer.publish(telemetry)

                print(
                    "Published telemetry: "
                    f"truck={telemetry.truck_id}, "
                    f"temperature={telemetry.temperature}°C, "
                    f"timestamp={telemetry.timestamp}"
                )

            producer.flush()
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\nStopping live telemetry producer.")
    finally:
        producer.flush()


if __name__ == "__main__":
    run_live_telemetry()