"""Run the StreamForge telemetry consumer."""

import time

from backend.api.routes.telemetry import add_telemetry
from backend.kafka.consumer import TelemetryConsumer
from backend.metrics.prometheus import (
    observe_processing_latency,
    record_invalid,
    record_late,
    record_processed,
    record_processing_error,
    record_received,
    set_active_trucks,
    set_events_in_progress,
    set_last_event_timestamp,
    set_processing_throughput,
    set_worker_status,
)
from backend.state.alerts_state import (
    get_active_alerts,
    update_temperature_alert,
)
from backend.state.changelog_recovery import restore_from_changelog
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

        # Ignore incomplete legacy/test records.
        if not isinstance(state, dict):
            continue

        required_fields = (
            "truck_id",
            "temperature",
            "timestamp",
        )

        if not all(field in state for field in required_fields):
            continue

        states.append(
            {
                "truck_id": state["truck_id"],
                "temperature": state["temperature"],
                "timestamp": state["timestamp"],
            }
        )

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


def has_local_truck_state(store: RocksDBStore) -> bool:
    """Return True when local truck state exists."""

    for key in store.keys():
        if key.startswith("truck:"):
            return True

    return False


def restore_state_if_needed(store: RocksDBStore) -> bool:
    """Restore local RocksDB state from Kafka when truck state is missing."""

    if has_local_truck_state(store):
        print(
            "Existing truck state found. "
            "Kafka changelog recovery not required."
        )
        return False

    print(
        "No local truck state found. "
        "Starting Kafka changelog recovery..."
    )

    restored_count = restore_from_changelog(store)

    print(
        "Kafka changelog recovery completed: "
        f"{restored_count} state records restored."
    )

    if restored_count > 0:
        print("Local RocksDB state successfully restored.")
        return True

    print("No state was available in the Kafka changelog.")
    return False


def timestamp_to_seconds(timestamp: object) -> float:
    """Convert an event timestamp to Unix seconds."""

    if hasattr(timestamp, "timestamp"):
        return float(timestamp.timestamp())

    try:
        return float(timestamp)
    except (TypeError, ValueError):
        return 0.0


def run() -> None:
    """Consume and process telemetry events continuously."""

    consumer = TelemetryConsumer(
        group_id="streamforge-telemetry-service",
        auto_offset_reset="latest",
    )

    store = RocksDBStore(STATE_PATH)

    # -------------------------------------------------------------
    # Worker monitoring state
    # -------------------------------------------------------------

    set_worker_status(True)
    set_events_in_progress(0)
    set_processing_throughput(0.0)

    # Throughput calculation window.
    throughput_window_start = time.perf_counter()
    throughput_event_count = 0

    try:
        # ---------------------------------------------------------
        # Worker startup recovery
        # ---------------------------------------------------------

        recovered_from_changelog = restore_state_if_needed(store)

        if recovered_from_changelog:
            print(
                "Worker startup recovery completed from "
                "Kafka state changelog."
            )

        # ---------------------------------------------------------
        # Restore active truck state
        # ---------------------------------------------------------

        active_truck_ids = get_active_trucks(store)

        set_active_trucks(len(active_truck_ids))

        set_metric(
            store,
            "active_trucks",
            len(active_truck_ids),
        )

        update_dashboard_snapshot(store)

        if active_truck_ids:
            print(
                "Restored active trucks: "
                f"{len(active_truck_ids)}"
            )
        else:
            print("No currently active trucks found.")

        # ---------------------------------------------------------
        # Restore Kafka processing position
        # ---------------------------------------------------------

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

        # ---------------------------------------------------------
        # Restore event-time watermark
        # ---------------------------------------------------------

        watermark = load_watermark(store)

        if watermark is not None:
            print(f"Watermark restored: {watermark}")
        else:
            print("No previous watermark found.")

        print("Telemetry consumer started.")

        # ---------------------------------------------------------
        # Main processing loop
        # ---------------------------------------------------------

        while True:
            telemetry = consumer.consume_one(timeout=1.0)

            # -----------------------------------------------------
            # No telemetry available
            # -----------------------------------------------------

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

            # -----------------------------------------------------
            # Start monitoring this event
            # -----------------------------------------------------

            processing_start = time.perf_counter()
            set_events_in_progress(1)

            try:
                # -------------------------------------------------
                # Event received
                # -------------------------------------------------

                record_received()

                increment_metric(
                    store,
                    "events_received",
                )

                # -------------------------------------------------
                # Stream processing
                # -------------------------------------------------

                processed = process_telemetry(telemetry)

                # -------------------------------------------------
                # Invalid event
                # -------------------------------------------------

                if processed is None:
                    record_invalid()

                    increment_metric(
                        store,
                        "events_invalid",
                    )

                    update_dashboard_snapshot(store)

                    print(
                        "Invalid telemetry rejected: "
                        f"truck={telemetry.truck_id}, "
                        f"temperature={telemetry.temperature}"
                    )

                    consumer.commit()

                    continue

                # -------------------------------------------------
                # Record latest valid event timestamp
                # -------------------------------------------------

                event_timestamp_seconds = timestamp_to_seconds(
                    processed.timestamp
                )

                if event_timestamp_seconds > 0:
                    set_last_event_timestamp(
                        event_timestamp_seconds
                    )

                # -------------------------------------------------
                # Establish initial watermark
                # -------------------------------------------------

                if watermark is None:
                    watermark = update_watermark(
                        store,
                        processed.timestamp,
                    )

                # -------------------------------------------------
                # Detect late event
                # -------------------------------------------------

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

                # -------------------------------------------------
                # Advance event-time watermark
                # -------------------------------------------------

                watermark = update_watermark(
                    store,
                    processed.timestamp,
                )

                # -------------------------------------------------
                # Persist five-minute window state
                # -------------------------------------------------

                window_start = get_five_minute_window_start(
                    processed.timestamp,
                )

                window_state = save_window_event(
                    store,
                    processed.truck_id,
                    window_start,
                    processed.temperature,
                )

                # -------------------------------------------------
                # Record successful processing
                # -------------------------------------------------

                record_processed()

                increment_metric(
                    store,
                    "events_processed",
                )

                # -------------------------------------------------
                # Persist truck state
                # -------------------------------------------------

                save_truck_state(
                    store,
                    processed,
                )

                # -------------------------------------------------
                # Create or clear temperature alert
                # -------------------------------------------------

                alert = update_temperature_alert(
                    store,
                    processed,
                )

                if alert is not None:
                    print(
                        "TEMPERATURE ALERT: "
                        f"truck={alert['truck_id']}, "
                        f"temperature={alert['temperature']}\\u00b0C, "
                        f"threshold={alert['threshold']}\\u00b0C"
                    )

                # -------------------------------------------------
                # Update active truck metrics
                # -------------------------------------------------

                active_truck_ids = get_active_trucks(store)

                set_active_trucks(len(active_truck_ids))

                set_metric(
                    store,
                    "active_trucks",
                    len(active_truck_ids),
                )

                # -------------------------------------------------
                # Save consumer recovery position
                # -------------------------------------------------

                position = consumer.get_last_position()

                if position is not None:
                    partition, offset = position

                    save_recovery_state(
                        store,
                        partition=partition,
                        offset=offset,
                    )

                # -------------------------------------------------
                # Update API telemetry state
                # -------------------------------------------------

                add_telemetry(processed)

                update_dashboard_snapshot(store)

                # -------------------------------------------------
                # Commit after persistence
                # -------------------------------------------------

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

            except Exception as exc:
                # -------------------------------------------------
                # Week 4 processing error metric
                # -------------------------------------------------

                record_processing_error()

                print(
                    "Stream processing error: "
                    f"{type(exc).__name__}: {exc}"
                )

                # Do not commit a message whose processing failed.
                continue

            finally:
                # -------------------------------------------------
                # Processing latency
                # -------------------------------------------------

                observe_processing_latency(
                    processing_start
                )

                # -------------------------------------------------
                # Event no longer in progress
                # -------------------------------------------------

                set_events_in_progress(0)

                # -------------------------------------------------
                # Processing throughput
                #
                # Counts all consumed events handled by the worker
                # over the current one-second measurement interval.
                # -------------------------------------------------

                throughput_event_count += 1

                elapsed = (
                    time.perf_counter()
                    - throughput_window_start
                )

                if elapsed >= 1.0:
                    throughput = (
                        throughput_event_count
                        / elapsed
                    )

                    set_processing_throughput(
                        throughput
                    )

                    throughput_event_count = 0
                    throughput_window_start = (
                        time.perf_counter()
                    )

    except KeyboardInterrupt:
        print("Stopping telemetry consumer.")

    finally:
        set_events_in_progress(0)
        set_worker_status(False)

        store.close()
        consumer.close()


if __name__ == "__main__":
    run()
