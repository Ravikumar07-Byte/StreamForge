import json
import os
from pathlib import Path

from bytewax import operators as op
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
from bytewax.outputs import DynamicSink, StatelessSinkPartition


INPUT_TOPIC = os.getenv(
    "KAFKA_INPUT_TOPIC",
    "streamforge-week2-benchmark",
)

BROKERS = [
    os.getenv("KAFKA_BROKER", "localhost:9092")
]

COUNT_DIR = Path(
    os.getenv(
        "BYTEWAX_BENCHMARK_COUNT_DIR",
        "benchmark_counts",
    )
)


def deserialize_message(msg):
    """Deserialize one Kafka telemetry message."""

    value = msg.value

    if isinstance(value, bytes):
        value = value.decode("utf-8")

    if isinstance(value, str):
        return json.loads(value)

    return value


def is_valid_temperature(item):
    """Week 2 filter: keep only temperature > 0."""

    try:
        return float(item.get("temperature", 0)) > 0
    except (TypeError, ValueError):
        return False


def normalize_event(item):
    """Week 2 map transformation."""

    return {
        "truck_id": str(item.get("truck_id", "")),
        "temperature": float(item.get("temperature", 0)),
        "timestamp": item.get("timestamp"),
    }


class CountingSinkPartition(StatelessSinkPartition):

    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.total = 0

        COUNT_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.path = (
            COUNT_DIR
            / f"worker_{self.worker_id}.count"
        )

        self.path.write_text(
            "0",
            encoding="utf-8",
        )

    def write_batch(self, items):
        self.total += len(items)

        tmp_path = self.path.with_suffix(".tmp")

        tmp_path.write_text(
            str(self.total),
            encoding="utf-8",
        )

        os.replace(
            tmp_path,
            self.path,
        )

    def close(self):
        pass


class CountingSink(DynamicSink):

    def build(
        self,
        step_id,
        worker_index,
        worker_count,
    ):
        process_id = os.getenv(
            "BYTEWAX_PROCESS_ID",
            str(worker_index),
        )

        return CountingSinkPartition(
            process_id
        )


flow = Dataflow(
    "streamforge_week2_benchmark"
)

kafka_input = kop.input(
    "consume",
    flow,
    brokers=BROKERS,
    topics=[INPUT_TOPIC],
    tail=True,
    batch_size=10000,
)

deserialized = op.map(
    "deserialize",
    kafka_input.oks,
    deserialize_message,
)

filtered = op.filter(
    "filter_temperature",
    deserialized,
    is_valid_temperature,
)

mapped = op.map(
    "map_normalize",
    filtered,
    normalize_event,
)

op.output(
    "benchmark_count",
    mapped,
    CountingSink(),
)
