"""Persistent temperature alert state for StreamForge."""

from datetime import datetime, timezone

from backend.models.telemetry import Telemetry
from backend.state.rocksdb_store import RocksDBStore


TEMPERATURE_ALERT_THRESHOLD = 35.0
ALERT_KEY_PREFIX = "alert:"


def update_temperature_alert(
    store: RocksDBStore,
    telemetry: Telemetry,
) -> dict[str, object] | None:
    """Create or update an alert when truck temperature is above threshold."""

    key = f"{ALERT_KEY_PREFIX}{telemetry.truck_id}"

    if telemetry.temperature > TEMPERATURE_ALERT_THRESHOLD:
        alert = {
            "truck_id": telemetry.truck_id,
            "temperature": telemetry.temperature,
            "threshold": TEMPERATURE_ALERT_THRESHOLD,
            "timestamp": telemetry.timestamp.isoformat(),
            "severity": "warning",
            "message": (
                f"High temperature detected: "
                f"{telemetry.temperature}°C"
            ),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        store.put(key, alert)
        return alert

    # Temperature returned to normal, so clear the active alert.
    if store.exists(key):
        store.delete(key)

    return None


def get_active_alerts(store: RocksDBStore) -> list[dict[str, object]]:
    """Return all currently active temperature alerts."""

    alerts: list[dict[str, object]] = []

    for key in store.keys():
        if not key.startswith(ALERT_KEY_PREFIX):
            continue

        alert = store.get(key)

        if isinstance(alert, dict):
            alerts.append(alert)

    alerts.sort(
        key=lambda alert: str(alert.get("updated_at", "")),
        reverse=True,
    )

    return alerts