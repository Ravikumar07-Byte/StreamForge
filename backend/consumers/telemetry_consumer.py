"""Run the StreamForge telemetry consumer."""

from backend.api.routes.telemetry import add_telemetry
from backend.kafka.consumer import TelemetryConsumer
from backend.metrics.prometheus import (
    record_invalid,
    record_processed,
    record_received,
    set_active_trucks,
)
from backend.state.metrics_state import (
    increment_metric,
    set_metric,
)
from backend.state.recovery import (
    load_recovery_state,
    save_recovery_state,
)
from backend.state.rocksdb_store import RocksDBStore
from backend.state.truck_state import (
    get_active_trucks,
    save_truck_state,
)
from backend.streaming.dataflow import process_telemetry


STATE_PATH = "data/state"


def run() -> None:
    """Consume and process telemetry events continuously."""

    consumer = TelemetryConsumer(
        group_id="streamforge-telemetry-service",
        auto_offset_reset="latest",
    )

    store = RocksDBStore(STATE_PATH)

    # Restore active trucks from persisted last-seen timestamps.
    active_truck_ids = get_active_trucks(store)
    set_active_trucks(len(active_truck_ids))
    set_metric(store, "active_trucks", len(active_truck_ids))

    if active_truck_ids:
        print(
            "Restored active trucks: "
            f"{len(active_truck_ids)}"
        )
    else:
        print("No currently active trucks found.")

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
                # Recalculate active trucks periodically even when
                # no new Kafka event arrives.
                active_truck_ids = get_active_trucks(store)

                set_active_trucks(len(active_truck_ids))
                set_metric(
                    store,
                    "active_trucks",
                    len(active_truck_ids),
                )

                continue

            # Record received event.
            record_received()
            increment_metric(
                store,
                "events_received",
            )

            processed = process_telemetry(telemetry)

            if processed is None:
                record_invalid()
                increment_metric(
                    store,
                    "events_invalid",
                )

                print(
                    "Invalid telemetry rejected: "
                    f"truck={telemetry.truck_id}, "
                    f"temperature={telemetry.temperature}"
                )
                continue

            # Record successfully processed event.
            record_processed()
            increment_metric(
                store,
                "events_processed",
            )

            save_truck_state(store, processed)

            # Update active truck count.
            active_truck_ids = get_active_trucks(store)

            set_active_trucks(len(active_truck_ids))
            set_metric(
                store,
                "active_trucks",
                len(active_truck_ids),
            )

            # Save Kafka recovery position.
            position = consumer.get_last_position()

            if position is not None:
                partition, offset = position

                save_recovery_state(
                    store,
                    partition=partition,
                    offset=offset,
                )

            # Keep recent telemetry available to the API.
            add_telemetry(processed)

            consumer.commit()

            print(
                "Processed telemetry: "
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