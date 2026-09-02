"""Run the StreamForge telemetry consumer."""

from backend.api.routes.telemetry import add_telemetry
from backend.kafka.consumer import TelemetryConsumer
from backend.metrics.prometheus import (
    record_invalid,
    record_processed,
    record_received,
    set_active_trucks,
)
from backend.state.recovery import (
    load_recovery_state,
    save_recovery_state,
)
from backend.state.rocksdb_store import RocksDBStore
from backend.state.truck_state import save_truck_state
from backend.streaming.dataflow import process_telemetry


STATE_PATH = "data/state"
TRUCK_STATE_PREFIX = "truck:"


def load_active_trucks(store: RocksDBStore) -> set[str]:
    """Restore truck IDs from persisted RocksDB state."""

    active_truck_ids: set[str] = set()

    for key in store.keys():
        if not key.startswith(TRUCK_STATE_PREFIX):
            continue

        truck_id = key[len(TRUCK_STATE_PREFIX):]

        if truck_id:
            active_truck_ids.add(truck_id)

    return active_truck_ids


def run() -> None:
    """Consume and process telemetry events continuously."""

    consumer = TelemetryConsumer(
        group_id="streamforge-telemetry-service",
        auto_offset_reset="latest",
    )

    store = RocksDBStore(STATE_PATH)

    # Restore previously known trucks from persistent state.
    active_truck_ids = load_active_trucks(store)
    set_active_trucks(len(active_truck_ids))

    if active_truck_ids:
        print(
            "Restored active trucks: "
            f"{len(active_truck_ids)}"
        )
    else:
        print("No persisted truck state found.")

    # Load the last persisted recovery position.
    recovery_state = load_recovery_state(store)

    if recovery_state is not None:
        print(
            "Recovery state loaded: "
            f"partition={recovery_state.get('partition')}, "
            f"offset={recovery_state.get('offset')}, "
            f"updated_at={recovery_state.get('updated_at')}"
        )
    else:
        print("No previous recovery state found.")

    print("Telemetry consumer started.")

    try:
        while True:
            telemetry = consumer.consume_one(timeout=1.0)

            if telemetry is None:
                continue

            # Record that Kafka delivered an event.
            record_received()

            # Process the telemetry through the streaming pipeline.
            processed = process_telemetry(telemetry)

            # Invalid telemetry is rejected by the pipeline.
            if processed is None:
                record_invalid()

                print(
                    f"Invalid telemetry rejected: "
                    f"truck={telemetry.truck_id}, "
                    f"temperature={telemetry.temperature}"
                )

                continue

            # Record successful processing.
            record_processed()

            # Update the persistent active-truck set.
            active_truck_ids.add(processed.truck_id)
            set_active_trucks(len(active_truck_ids))

            # Persist the latest state for this truck.
            save_truck_state(store, processed)

            # Get the Kafka position of this event.
            position = consumer.get_last_position()

            if position is not None:
                partition, offset = position

                # Persist the processing position.
                save_recovery_state(
                    store,
                    partition=partition,
                    offset=offset,
                )

            # Make processed telemetry available to the API/dashboard.
            add_telemetry(processed)

            # Commit Kafka only after successful processing and
            # persistent state update.
            consumer.commit()

            print(
                f"Processed telemetry: "
                f"truck={processed.truck_id}, "
                f"temperature={processed.temperature}, "
                f"timestamp={processed.timestamp}"
            )

    except KeyboardInterrupt:
        print("Stopping telemetry consumer.")

    finally:
        store.close()
        consumer.close()


if __name__ == "__main__":
    run()