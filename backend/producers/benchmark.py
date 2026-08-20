"""Benchmark StreamForge Kafka telemetry publishing."""

import time

from backend.kafka.producer import TelemetryProducer
from backend.producers.telemetry_generator import (
    generate_telemetry,
    generate_truck_ids,
)


def run_benchmark(truck_count: int = 1000) -> None:
    """Generate and publish telemetry for a number of trucks."""

    producer = TelemetryProducer()
    truck_ids = generate_truck_ids(truck_count)

    start = time.perf_counter()

    events = [
        generate_telemetry(truck_id)
        for truck_id in truck_ids
    ]

    producer.publish_batch(events)

    elapsed = time.perf_counter() - start
    throughput = truck_count / elapsed if elapsed > 0 else 0

    print(f"Messages: {truck_count}")
    print(f"Elapsed: {elapsed:.4f} seconds")
    print(f"Throughput: {throughput:.2f} events/sec")


if __name__ == "__main__":
    run_benchmark()
