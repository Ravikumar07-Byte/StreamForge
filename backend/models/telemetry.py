from datetime import datetime, timezone

from pydantic import BaseModel, Field


class Telemetry(BaseModel):
    """Truck temperature telemetry event."""

    truck_id: str = Field(min_length=1)
    temperature: float
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
