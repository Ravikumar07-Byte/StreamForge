"""Run the StreamForge telemetry consumer."""

from backend.api.routes.telemetry import add_telemetry
from backend.kafka.consumer import TelemetryConsumer
from backend.metrics.prometheus import (
    record_invalid,
    record_late,
    record_processed,
    record_received,
    set_active_trucks,
)
from backend.state.late_events import save_late_event
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
from backend.state.watermark import load_watermark, update_watermark
from backend.state.window_state import save_window_event
from backend.streaming.dataflow import process_telemetry
from backend.streaming.windowing import (
    get_five_minute_window_start,
    is_late_event,
)


STATE_PATH = "data/state"
ALLOWED_LATENESS_SECONDS = 60


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

    watermark = load_watermark(store)

    if watermark is not None:
        print(
            "Watermark restored: "
            f"{watermark}"
        )
    else:
        print("No previous watermark found.")

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

                consumer.commit()
                continue

            # Establish an initial watermark from the first valid event.
            if watermark is None:
                watermark = update_watermark(
                    store,
                    processed.timestamp,
                )

            # Detect events that are older than the allowed lateness
            # relative to the current event-time watermark.
            if is_late_event(
                processed.timestamp,
                watermark,
                ALLOWED_LATENESS_SECONDS,
            ):
                record_late()
                increment_metric(
                    store,
                    "events_late",
                )

                save_late_event(
                    store,
                    processed,
                    watermark,
                )

                print(
                    "Late telemetry event: "
                    f"truck={processed.truck_id}, "
                    f"timestamp={processed.timestamp}, "
                    f"watermark={watermark}"
                )

                # The event has been handled and persisted, so commit
                # its Kafka offset without adding it to the active window.
                consumer.commit()
                continue

            # Advance the event-time watermark for an on-time event.
            watermark = update_watermark(
                store,
                processed.timestamp,
            )

            # Add the event to its persistent five-minute window.
            window_start = get_five_minute_window_start(
                processed.timestamp,
            )

            window_state = save_window_event(
                store,
                processed.truck_id,
                window_start,
                processed.temperature,
            )

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
                f"timestamp={processed.timestamp}, "
                f"window_start={window_start}, "
                f"window_count={window_state['event_count']}, "
                f"window_average={window_state['temperature_average']}"
            )

    except KeyboardInterrupt:
        print("Stopping telemetry consumer.")

    finally:
        store.close()
        consumer.close()


if __name__ == "__main__":
    run()
