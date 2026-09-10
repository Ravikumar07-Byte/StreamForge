"""
StreamForge Week 2 - Bytewax streaming pipeline.

Processing graph:

Kafka
  ↓
Consume
  ↓
Deserialize
  ↓
Filter: temperature > 0°C
  ↓
Map: normalize telemetry
  ↓
Key by truck_id
  ↓
5-minute event-time tumbling window
  ↓
Per-truck average temperature
  ↓
Kafka output

Late events are routed to a dedicated Kafka topic.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import bytewax.operators as op
from bytewax.connectors.kafka import (
    KafkaSinkMessage,
    operators as kop,
)
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import (
    EventClock,
    TumblingWindower,
    fold_window,
)


KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "truck-telemetry")

PROCESSED_TOPIC = os.getenv(
    "KAFKA_PROCESSED_TOPIC",
    "truck-telemetry-processed",
)

WINDOW_TOPIC = os.getenv(
    "KAFKA_WINDOW_TOPIC",
    "truck-telemetry-5m-average",
)

LATE_TOPIC = os.getenv(
    "KAFKA_LATE_TOPIC",
    "truck-telemetry-late",
)

WINDOW_SIZE = timedelta(minutes=5)

# Give the event-time watermark 30 seconds of allowed lateness.
WAIT_FOR_LATE_DATA = timedelta(seconds=30)

# Stable UTC alignment for 5-minute tumbling windows.
WINDOW_ALIGNMENT = datetime(
    1970,
    1,
    1,
    tzinfo=timezone.utc,
)


def deserialize_message(message: Any) -> dict[str, Any] | None:
    """Deserialize a Kafka telemetry message."""

    try:
        if message.value is None:
            return None

        payload = json.loads(message.value.decode("utf-8"))

        if not isinstance(payload, dict):
            return None

        truck_id = payload.get("truck_id")
        temperature = payload.get("temperature")
        timestamp = payload.get("timestamp")

        if truck_id is None:
            return None

        if temperature is None:
            return None

        if timestamp is None:
            return None

        return {
            "truck_id": str(truck_id),
            "temperature": float(temperature),
            "timestamp": timestamp,
        }

    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        return None


def filter_positive_temperature(
    telemetry: dict[str, Any],
) -> bool:
    """
    Week 2 specification:
    Filter events where Temp > 0.
    """
    return telemetry["temperature"] > 0.0


def parse_timestamp(timestamp: str) -> datetime:
    """Convert an ISO timestamp to an aware UTC datetime."""

    value = timestamp

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def map_telemetry(
    telemetry: dict[str, Any],
) -> dict[str, Any]:
    """
    Map stage.

    Normalizes temperature and converts timestamp into
    an aware UTC datetime for event-time windowing.
    """

    timestamp = parse_timestamp(
        str(telemetry["timestamp"])
    )

    return {
        "truck_id": telemetry["truck_id"],
        "temperature": round(
            float(telemetry["temperature"]),
            2,
        ),
        "timestamp": timestamp,
    }


def add_temperature(
    accumulator: dict[str, float],
    telemetry: dict[str, Any],
) -> dict[str, float]:
    """Add one telemetry event to the window accumulator."""

    return {
        "temperature_sum": (
            accumulator["temperature_sum"]
            + telemetry["temperature"]
        ),
        "event_count": (
            accumulator["event_count"] + 1
        ),
    }


def merge_temperature(
    left: dict[str, float],
    right: dict[str, float],
) -> dict[str, float]:
    """Merge two window accumulators."""

    return {
        "temperature_sum": (
            left["temperature_sum"]
            + right["temperature_sum"]
        ),
        "event_count": (
            left["event_count"]
            + right["event_count"]
        ),
    }


def build_average_result(
    item: tuple[str, tuple[int, dict[str, float]]],
) -> dict[str, Any]:
    """Convert a Bytewax window result into JSON-ready output."""

    truck_id, (window_id, state) = item

    count = int(state["event_count"])
    total = float(state["temperature_sum"])

    average = total / count if count else 0.0

    return {
        "truck_id": truck_id,
        "window_id": window_id,
        "event_count": count,
        "temperature_sum": round(total, 2),
        "average_temperature": round(average, 2),
    }


def to_json_bytes(
    payload: dict[str, Any],
) -> KafkaSinkMessage:
    """Serialize a dictionary to Kafka."""

    value = json.dumps(
        payload,
        default=lambda value: (
            value.isoformat()
            if isinstance(value, datetime)
            else value
        ),
    ).encode("utf-8")

    key = str(
        payload.get("truck_id", "")
    ).encode("utf-8")

    return KafkaSinkMessage(
        key=key,
        value=value,
    )


def build_flow() -> Dataflow:
    """Build the StreamForge Week-2 Bytewax dataflow."""

    flow = Dataflow("streamforge-week2")

    # ---------------------------------------------------------
    # 1. CONSUME
    # ---------------------------------------------------------

    kafka_input = kop.input(
        "consume",
        flow,
        brokers=KAFKA_BROKERS,
        topics=[INPUT_TOPIC],
        tail=True,
        batch_size=5000,
    )

    messages = kafka_input.oks

    # ---------------------------------------------------------
    # 2. DESERIALIZE
    # ---------------------------------------------------------

    telemetry = op.filter_map(
        "deserialize",
        messages,
        deserialize_message,
    )

    # ---------------------------------------------------------
    # 3. FILTER
    # ---------------------------------------------------------

    positive_temperature = op.filter(
        "filter-temperature-positive",
        telemetry,
        filter_positive_temperature,
    )

    # ---------------------------------------------------------
    # 4. MAP
    # ---------------------------------------------------------

    mapped = op.map(
        "map-normalize-telemetry",
        positive_temperature,
        map_telemetry,
    )

    # Publish the filtered + mapped stream so the topology
    # can be independently observed.
    processed_messages = op.map(
        "serialize-processed",
        mapped,
        to_json_bytes,
    )

    kop.output(
        "processed-output",
        processed_messages,
        brokers=KAFKA_BROKERS,
        topic=PROCESSED_TOPIC,
    )

    # ---------------------------------------------------------
    # 5. KEY BY TRUCK
    # ---------------------------------------------------------

    keyed = op.key_on(
        "key-by-truck",
        mapped,
        lambda item: item["truck_id"],
    )

    # ---------------------------------------------------------
    # 6. EVENT-TIME CLOCK
    # ---------------------------------------------------------

    clock = EventClock(
        ts_getter=lambda item: item["timestamp"],
        wait_for_system_duration=WAIT_FOR_LATE_DATA,
    )

    # ---------------------------------------------------------
    # 7. FIVE-MINUTE TUMBLING WINDOW
    # ---------------------------------------------------------

    windower = TumblingWindower(
        length=WINDOW_SIZE,
        align_to=WINDOW_ALIGNMENT,
    )

    # ---------------------------------------------------------
    # 8. FIVE-MINUTE AVERAGE
    # ---------------------------------------------------------

    windowed = fold_window(
        "five-minute-average",
        keyed,
        clock,
        windower,
        builder=lambda: {
            "temperature_sum": 0.0,
            "event_count": 0,
        },
        folder=add_temperature,
        merger=merge_temperature,
        ordered=True,
    )

    averages = op.map(
        "format-window-average",
        windowed.down,
        build_average_result,
    )

    average_messages = op.map(
        "serialize-window-average",
        averages,
        to_json_bytes,
    )

    kop.output(
        "window-average-output",
        average_messages,
        brokers=KAFKA_BROKERS,
        topic=WINDOW_TOPIC,
    )

    # ---------------------------------------------------------
    # 9. LATE EVENTS
    # ---------------------------------------------------------

    late_messages = op.map(
        "format-late-event",
        windowed.late,
        lambda item: {
            "truck_id": item[0],
            "late_event": item[1],
        },
    )

    late_kafka_messages = op.map(
        "serialize-late-event",
        late_messages,
        to_json_bytes,
    )

    kop.output(
        "late-event-output",
        late_kafka_messages,
        brokers=KAFKA_BROKERS,
        topic=LATE_TOPIC,
    )

    return flow


flow = build_flow()
