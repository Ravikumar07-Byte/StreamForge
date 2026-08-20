"""Generate synthetic truck telemetry for StreamForge."""

import random
from datetime import datetime, timezone

from backend.models.telemetry import Telemetry


def generate_telemetry(truck_id: str) -> Telemetry:
    """Generate one telemetry event for a truck."""

    return Telemetry(
        truck_id=truck_id,
        temperature=round(random.uniform(15.0, 45.0), 2),
        timestamp=datetime.now(timezone.utc),
    )


def generate_truck_ids(count: int) -> list[str]:
    """Generate truck identifiers."""

    return [
        f"TRUCK-{number:06d}"
        for number in range(1, count + 1)
    ]
