"""Shared dashboard snapshot storage for StreamForge."""

import json
import os
import time
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

SNAPSHOT_PATH = Path("data/dashboard_state.json")

DEFAULT_SNAPSHOT: dict[str, Any] = {
    "kafka_status": "Online",
    "telemetry": [],
    "alerts": [],
    "metrics": {
        "events_received": 0,
        "events_processed": 0,
        "events_invalid": 0,
        "events_late": 0,
        "active_trucks": 0,
    },
}


def save_snapshot(data: dict[str, Any]) -> None:
    """Atomically save the latest dashboard snapshot with Windows retry support."""

    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)

    temp_path: Path | None = None

    try:
        with NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=SNAPSHOT_PATH.parent,
            prefix="dashboard_state_",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            json.dump(data, temp_file, indent=2)
            temp_file.flush()
            os.fsync(temp_file.fileno())
            temp_path = Path(temp_file.name)

        last_error: PermissionError | None = None

        for attempt in range(10):
            try:
                os.replace(temp_path, SNAPSHOT_PATH)
                return
            except PermissionError as error:
                last_error = error

                if attempt < 9:
                    time.sleep(0.1)

        if last_error is not None:
            raise last_error

    finally:
        if temp_path is not None and temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass


def load_snapshot() -> dict[str, Any]:
    """Load the latest dashboard snapshot."""

    if not SNAPSHOT_PATH.exists():
        return DEFAULT_SNAPSHOT.copy()

    try:
        with SNAPSHOT_PATH.open("r", encoding="utf-8") as file:
            data = json.load(file)

        if isinstance(data, dict):
            return data

    except (OSError, json.JSONDecodeError):
        pass

    return DEFAULT_SNAPSHOT.copy()