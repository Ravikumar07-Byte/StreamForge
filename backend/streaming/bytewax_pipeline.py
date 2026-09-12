"""
StreamForge Week 2 - Bytewax Streaming Pipeline

Processing graph:

Kafka
  |
  v
Consume
  |
  v
Deserialize
  |
  v
Filter: temperature > 0
  |
  v
Map / Normalize
  |
  +-----------------------> Processed Kafka topic
  |
  v
Key by Truck
  |
  v
5-minute Event-Time Tumbling Window
  |
  v
Average Temperature
  |
  +-----------------------> 5-minute Average Kafka topic

Late events
  |
  +-----------------------> Late Kafka topic
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import bytewax.operators as op
from bytewax.connectors.kafka import KafkaSinkMessage
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
from bytewax.operators.windowing import (
    EventClock,
    TumblingWindower,
    fold_window,
)


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BROKERS = os.getenv(
    "KAFKA_BROKERS",
    "localhost:9092",
).split(",")

INPUT_TOPIC = os.getenv(
    "KAFKA_INPUT_TOPIC",
    "truck-telemetry",
)

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

KAFKA_BATCH_SIZE = int(
    os.getenv(
        "STREAMFORGE_KAFKA_BATCH_SIZE",
        "10000",
    )
)

WINDOW_SIZE = timedelta(minutes=5)

WAIT_FOR_LATE_DATA = timedelta(seconds=30)

WINDOW_ALIGNMENT = datetime(
    1970,
    1,
    1,
    tzinfo=timezone.utc,
)


# ============================================================
# DESERIALIZATION
# ============================================================

def deserialize_message(
    message: Any,
) -> dict[str, Any] | None:
    """
    Deserialize one Kafka message.

    Expected telemetry fields:
        truck_id
        temperature
        timestamp
    """

    try:
        if message.value is None:
            return None

        payload = json.loads(
            message.value.decode("utf-8")
        )

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
            "timestamp": str(timestamp),
        }

    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ):
        return None


# ============================================================
# FILTER
# ============================================================

def filter_positive_temperature(
    telemetry: dict[str, Any],
) -> bool:
    """
    Week 2 requirement:
    Filter out temperatures <= 0.
    """

    return telemetry["temperature"] > 0.0


# ============================================================
# TIMESTAMP
# ============================================================

def parse_timestamp(
    timestamp: str,
) -> datetime:
    """Convert ISO timestamp to UTC-aware datetime."""

    value = timestamp

    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        parsed = parsed.replace(
            tzinfo=timezone.utc
        )

    return parsed.astimezone(
        timezone.utc
    )


# ============================================================
# MAP / NORMALIZATION
# ============================================================

def map_telemetry(
    telemetry: dict[str, Any],
) -> dict[str, Any]:
    """
    Normalize telemetry after filtering.
    """

    timestamp = parse_timestamp(
        telemetry["timestamp"]
    )

    return {
        "truck_id": telemetry["truck_id"],
        "temperature": round(
            float(
                telemetry["temperature"]
            ),
            2,
        ),
        "timestamp": timestamp,
    }


# ============================================================
# WINDOW AGGREGATION
# ============================================================

def add_temperature(
    accumulator: dict[str, float],
    telemetry: dict[str, Any],
) -> dict[str, float]:
    """
    Add one event to the 5-minute accumulator.
    """

    return {
        "temperature_sum": (
            accumulator["temperature_sum"]
            + telemetry["temperature"]
        ),
        "event_count": (
            accumulator["event_count"]
            + 1
        ),
    }


def merge_temperature(
    left: dict[str, float],
    right: dict[str, float],
) -> dict[str, float]:
    """
    Merge two partial window accumulators.
    """

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


# ============================================================
# WINDOW RESULT
# ============================================================

def build_average_result(
    item: tuple[
        str,
        tuple[int, dict[str, float]],
    ],
) -> dict[str, Any]:
    """
    Calculate mathematically correct average:

        average = temperature_sum / event_count
    """

    truck_id, (
        window_id,
        state,
    ) = item

    count = int(
        state["event_count"]
    )

    total = float(
        state["temperature_sum"]
    )

    average = (
        total / count
        if count > 0
        else 0.0
    )

    return {
        "truck_id": truck_id,
        "window_id": window_id,
        "event_count": count,
        "temperature_sum": round(
            total,
            2,
        ),
        "average_temperature": round(
            average,
            2,
        ),
    }


# ============================================================
# KAFKA SERIALIZATION
# ============================================================

def to_json_bytes(
    payload: dict[str, Any],
) -> KafkaSinkMessage:
    """
    Serialize dictionary to Kafka message.
    """

    value = json.dumps(
        payload,
        separators=(",", ":"),
        default=lambda value: (
            value.isoformat()
            if isinstance(
                value,
                datetime,
            )
            else value
        ),
    ).encode("utf-8")

    key = str(
        payload.get(
            "truck_id",
            "",
        )
    ).encode("utf-8")

    return KafkaSinkMessage(
        key=key,
        value=value,
    )


# ============================================================
# BUILD BYTEWAX FLOW
# ============================================================

def build_flow() -> Dataflow:
    """
    Build StreamForge Week 2 Bytewax flow.
    """

    flow = Dataflow(
        "streamforge-week2"
    )

    # --------------------------------------------------------
    # 1. CONSUME
    # --------------------------------------------------------

    kafka_input = kop.input(
        "consume",
        flow,
        brokers=KAFKA_BROKERS,
        topics=[INPUT_TOPIC],
        tail=True,
        batch_size=KAFKA_BATCH_SIZE,
    )

    messages = kafka_input.oks

    # --------------------------------------------------------
    # 2. DESERIALIZE
    # --------------------------------------------------------

    telemetry = op.filter_map(
        "deserialize",
        messages,
        deserialize_message,
    )

    # --------------------------------------------------------
    # 3. FILTER
    # --------------------------------------------------------

    positive_temperature = op.filter(
        "filter-temperature-positive",
        telemetry,
        filter_positive_temperature,
    )

    # --------------------------------------------------------
    # 4. MAP
    # --------------------------------------------------------

    mapped = op.map(
        "map-normalize-telemetry",
        positive_temperature,
        map_telemetry,
    )

    # --------------------------------------------------------
    # 5. PROCESSED OUTPUT
    #
    # IMPORTANT:
    # benchmark_100k.py uses this topic to determine
    # how many events Bytewax processed.
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # 6. KEY BY TRUCK
    # --------------------------------------------------------

    keyed = op.key_on(
        "key-by-truck",
        mapped,
        lambda item: item["truck_id"],
    )

    # --------------------------------------------------------
    # 7. EVENT-TIME CLOCK
    # --------------------------------------------------------

    clock = EventClock(
        ts_getter=lambda item: item["timestamp"],
        wait_for_system_duration=WAIT_FOR_LATE_DATA,
    )

    # --------------------------------------------------------
    # 8. 5-MINUTE TUMBLING WINDOW
    # --------------------------------------------------------

    windower = TumblingWindower(
        length=WINDOW_SIZE,
        align_to=WINDOW_ALIGNMENT,
    )

    # --------------------------------------------------------
    # 9. FIVE-MINUTE AVERAGE
    # --------------------------------------------------------

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

    # --------------------------------------------------------
    # 10. LATE EVENTS
    # --------------------------------------------------------

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


# ============================================================
# BYTEWAX ENTRY POINT
# ============================================================

flow = build_flow()
