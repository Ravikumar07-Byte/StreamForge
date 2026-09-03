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
from backend.state.alerts_state import (
    get_active_alerts,
    update_temperature_alert,
)
from backend.state.late_events import save_late_event
from backend.state.snapshot import save_snapshot
from backend.state.metrics_state import (
    increment_metric,
    load_metrics,
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


def update_dashboard_snapshot(store: RocksDBStore) -> None:
    """Write the latest dashboard state for the API."""

    states: list[dict[str, object]] = []

    for key in store.keys():
        if not key.startswith("truck:"):
            continue

        truck_id = key[len("truck:"):]
        if not truck_id:
            continue

        state = store.get(key)

        if isinstance(state, dict):
            states.append(state)

    metrics = load_metrics(store)
    alerts = get_active_alerts(store)

    save_snapshot(
        {
            "kafka_status": "Online",
            "telemetry": [
                {
                    "truck": state["truck_id"],
                    "temperature": state["temperature"],
                    "timestamp": state["timestamp"],
                }
                for state in states
            ],
            "alerts": alerts,
            "metrics": metrics,
        }
    )


def run() -> None:
    """Consume and process telemetry events continuously."""

    consumer = TelemetryConsumer(
        group_id="streamforge-telemetry-service",
        auto_offset_reset="latest",
    )

    store = RocksDBStore(STATE_PATH)

    active_truck_ids = get_active_trucks(store)

    set_active_trucks(len(active_truck_ids))
    set_metric(store, "active_trucks", len(active_truck_ids))

    update_dashboard_snapshot(store)

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
        print(f"Watermark restored: {watermark}")
    else:
        print("No previous watermark found.")

    print("Telemetry consumer started.")

    try:
        while True:
            telemetry = consumer.consume_one(timeout=1.0)

            if telemetry is None:
                active_truck_ids = get_active_trucks(store)

                set_active_trucks(len(active_truck_ids))
                set_metric(
                    store,
                    "active_trucks",
                    len(active_truck_ids),
                )

                update_dashboard_snapshot(store)
                continue

            record_received()
            increment_metric(store, "events_received")

            processed = process_telemetry(telemetry)

            if processed is None:
                record_invalid()
                increment_metric(store, "events_invalid")

                update_dashboard_snapshot(store)

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
            increment_metric(store, "events_processed")

            save_truck_state(store, processed)

            # Create or clear a temperature alert.
            alert = update_temperature_alert(
                store,
                processed,
            )

            if alert is not None:
                print(
                    "TEMPERATURE ALERT: "
                    f"truck={alert['truck_id']}, "
                    f"temperature={alert['temperature']}°C, "
                    f"threshold={alert['threshold']}°C"
                )

            active_truck_ids = get_active_trucks(store)

            set_active_trucks(len(active_truck_ids))
            set_metric(
                store,
                "active_trucks",
                len(active_truck_ids),
            )

            position = consumer.get_last_position()

            if position is not None:
                partition, offset = position

                save_recovery_state(
                    store,
                    partition=partition,
                    offset=offset,
                )

            add_telemetry(processed)

            update_dashboard_snapshot(store)

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