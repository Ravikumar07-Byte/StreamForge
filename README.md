Yes. Below is **one single Markdown block** containing Day 0 through Day 4 together. I also corrected the formatting so you can **copy-paste the entire block directly into `README.md`**.

````markdown
# StreamForge

## Day 0 — Project Initialization

### Overview

Day 0 marks the starting point of the **StreamForge** project. The repository was initialized with the basic project structure and development environment required to begin building the real-time truck telemetry streaming system.

At this stage, the `README.md` file was still empty, and the repository contained the initial project folders and configuration files.

### Initial Project Structure

```text
StreamForge/
│
├── .github/
├── backend/
├── config/
├── data/
├── docker/
├── docs/
├── frontend/
├── scripts/
├── state/
├── tests/
│
├── .env.example
├── .gitignore
├── docker-compose.yml
├── LICENSE
├── README.md
└── requirements.txt
````

### Day 0 Objectives

* Initialize the StreamForge repository.
* Establish the basic project directory structure.
* Prepare the Python development environment.
* Add the initial dependency configuration.
* Add environment configuration template.
* Add Git ignore configuration.
* Prepare directories for backend, frontend, data, documentation, scripts, state management, and testing.
* Initialize the project under Git version control.

### Initial Technology Direction

The project structure was prepared for a system involving:

* Python backend development
* Kafka-based event streaming
* Docker-based infrastructure
* Automated testing
* Frontend integration
* Configuration management
* Data and state management

### Git

Initial commit:

```text
chore: initialize StreamForge project structure and environment
```

Commit:

```text
76c7249
```

### Day 0 Status

* [x] Repository initialized
* [x] Project structure created
* [x] Backend directory prepared
* [x] Frontend directory prepared
* [x] Configuration directory prepared
* [x] Data directory prepared
* [x] Docker directory prepared
* [x] Documentation directory prepared
* [x] Scripts directory prepared
* [x] State directory prepared
* [x] Tests directory prepared
* [x] `requirements.txt` created
* [x] `.env.example` created
* [x] `.gitignore` created
* [x] `LICENSE` added
* [x] README documentation started

### Day 0 Result

The StreamForge repository and development foundation were established successfully. The project was ready to move into the next stage: **Kafka infrastructure setup and backend implementation**.

---

## Day 1 — Backend: Local Kafka Infrastructure

### Overview

Day 1 focused on establishing the **backend messaging infrastructure** for StreamForge.

The backend uses Apache Kafka as the event-streaming layer. Kafka was configured locally with Docker Compose, and the `truck-telemetry` topic was created as the communication channel for truck telemetry events.

### Backend Objectives

* Set up Apache Kafka locally using Docker.
* Configure Kafka as a single-node broker.
* Expose Kafka on port `9092`.
* Create the `truck-telemetry` topic.
* Configure the topic with four partitions.
* Verify that the Kafka broker is running correctly.
* Verify Kafka connectivity from the local development environment.
* Verify the Kafka topic configuration.

### Backend Components Completed

* Docker-based Kafka broker
* Kafka connection on `localhost:9092`
* `truck-telemetry` Kafka topic
* Four Kafka partitions
* Local Kafka connectivity verification
* Topic configuration verification

### Docker Compose

Apache Kafka was configured to run locally through Docker Compose.

Kafka was started using:

```powershell
docker compose up -d
```

The running container was verified using:

```powershell
docker compose ps
```

The Kafka service was available as:

```text
streamforge-kafka
```

### Kafka Connectivity

Kafka connectivity was verified using:

```powershell
Test-NetConnection localhost -Port 9092
```

The connection was successfully established:

```text
TcpTestSucceeded : True
```

This confirmed that the Kafka broker was accessible through:

```text
localhost:9092
```

### Telemetry Topic

The dedicated Kafka topic for truck telemetry was created:

```text
truck-telemetry
```

The topic was configured with four partitions:

```text
Partition 0
Partition 1
Partition 2
Partition 3
```

The topic configuration was verified using:

```powershell
docker exec streamforge-kafka /opt/kafka/bin/kafka-topics.sh --describe --topic truck-telemetry --bootstrap-server localhost:9092
```

### Backend Flow

```text
Truck Telemetry
       │
       ▼
Backend Producer
       │
       ▼
Apache Kafka
       │
       ▼
truck-telemetry
       │
       ▼
Backend Consumer
```

### Day 1 Backend Status

* [x] Docker-based Kafka broker
* [x] Kafka broker running locally
* [x] Kafka exposed on port `9092`
* [x] `truck-telemetry` topic created
* [x] Four Kafka partitions configured
* [x] Kafka connectivity verified
* [x] Topic configuration verified
* [ ] Telemetry data model
* [ ] Kafka producer
* [ ] Kafka consumer
* [ ] Telemetry processing pipeline
* [ ] API layer
* [ ] Database/storage integration

### Git

Commit:

```text
feat: add local Kafka infrastructure
```

Commit:

```text
d18763d
```

### Day 1 Result

The local Kafka infrastructure was successfully established for StreamForge. The backend now has a running Kafka broker and a dedicated `truck-telemetry` topic ready for the next backend development stages.

---

## Day 2 — Backend: Truck Telemetry Data Model

### Overview

Day 2 focused on creating the **backend data model** for StreamForge truck telemetry events.

A Pydantic `Telemetry` model was introduced to define and validate the structure of telemetry data before it is sent through the Kafka streaming pipeline.

### Backend Objectives

* Create the truck telemetry data model.
* Define the required truck identifier.
* Define the truck temperature field.
* Automatically generate a UTC timestamp.
* Validate telemetry input using Pydantic.
* Serialize telemetry events into JSON.
* Add automated tests for the telemetry model.

### Telemetry Data Model

The backend telemetry model was created in:

```text
backend/
└── models/
    └── telemetry.py
```

The model contains three fields:

| Field         | Type       | Purpose                                      |
| ------------- | ---------- | -------------------------------------------- |
| `truck_id`    | `str`      | Identifies the truck                         |
| `temperature` | `float`    | Stores truck temperature telemetry           |
| `timestamp`   | `datetime` | Records when the telemetry event was created |

### Model Implementation

```python
from datetime import datetime, timezone

from pydantic import BaseModel, Field


class Telemetry(BaseModel):
    """Truck temperature telemetry event."""

    truck_id: str = Field(min_length=1)
    temperature: float
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
```

### Validation

The `truck_id` field requires at least one character.

Therefore, an empty truck ID is rejected by the model.

Example:

```python
Telemetry(
    truck_id="TRUCK-000001",
    temperature=32.5,
)
```

The model automatically creates the timestamp when it is not supplied.

### JSON Serialization

The telemetry model supports JSON serialization using Pydantic:

```python
telemetry.model_dump_json()
```

This allows the validated telemetry object to be converted into a JSON payload suitable for Kafka messaging.

### Backend Data Flow

```text
Truck Telemetry Input
        │
        ▼
Pydantic Telemetry Model
        │
        ├── truck_id
        ├── temperature
        └── timestamp
        │
        ▼
Validated Telemetry Object
        │
        ▼
JSON Serialization
        │
        ▼
Kafka Producer
```

### Tests

Tests were added in:

```text
tests/
└── test_producer.py
```

The tests verify:

* Telemetry object creation
* Truck ID validation
* Temperature value handling
* Automatic timestamp creation
* JSON serialization
* Rejection of an empty truck ID

### Test Result

The telemetry model tests were successfully executed.

```text
3 passed
```

The complete project test suite at this stage also passed:

```text
4 passed
```

### Backend Components Completed

* [x] Truck telemetry Pydantic model
* [x] Truck ID validation
* [x] Temperature field
* [x] Automatic UTC timestamp
* [x] JSON serialization
* [x] Telemetry model tests
* [x] Empty truck ID validation

### Git

Commit:

```text
feat: add truck telemetry data model
```

Commit:

```text
49b2f81
```

### Day 2 Result

The StreamForge backend now has a validated and testable **truck telemetry data model**. This provides a consistent structure for telemetry events before they are serialized and published to Kafka.

---

## Day 3 — Backend: Kafka Configuration and Administration

### Overview

Day 3 focused on adding the **Kafka configuration and administration layer** to the StreamForge backend.

The backend was extended with centralized Kafka configuration, topic definitions, and an administration utility for checking Kafka broker availability.

### Backend Objectives

* Create centralized Kafka configuration.
* Load Kafka settings from environment variables.
* Define the telemetry Kafka topic through configuration.
* Create a reusable Kafka `AdminClient`.
* Add a Kafka broker availability check.
* Verify the backend can communicate with the local Kafka broker.
* Maintain automated testing through the existing CI workflow.

### Kafka Configuration

The Kafka configuration was created in:

```text
backend/
└── kafka/
    ├── config.py
    ├── topics.py
    └── admin.py
```

### Kafka Environment Configuration

The backend reads Kafka configuration using environment variables.

The default local configuration is:

```text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TELEMETRY_TOPIC=truck-telemetry
KAFKA_PRODUCER_CLIENT_ID=streamforge-telemetry-producer
```

The configuration provides:

* Kafka bootstrap server address
* Telemetry topic name
* Producer client ID

Environment variables allow the Kafka configuration to be changed without modifying the backend source code.

### Kafka Topic Definition

The telemetry topic is exposed through a backend constant:

```python
from backend.kafka.config import KAFKA_TELEMETRY_TOPIC

TRUCK_TELEMETRY_TOPIC = KAFKA_TELEMETRY_TOPIC
```

This gives the backend a single reusable reference to the truck telemetry topic.

### Kafka Administration

A Kafka administration utility was added using `confluent_kafka.admin.AdminClient`.

The backend provides:

```python
def create_admin_client() -> AdminClient:
    ...
```

and:

```python
def kafka_is_available() -> bool:
    ...
```

The availability function attempts to communicate with the Kafka broker and returns:

```text
True
```

when the broker is reachable.

### Kafka Availability Verification

The backend Kafka connection was tested using:

```powershell
python -c "from backend.kafka.admin import kafka_is_available; print('Kafka available:', kafka_is_available())"
```

The result was:

```text
Kafka available: True
```

This confirmed that the StreamForge backend could communicate with the local Kafka broker.

### Configuration Verification

The configured Kafka values were also verified:

```text
localhost:9092
truck-telemetry
```

### Backend Architecture

```text
StreamForge Backend
        │
        ▼
Kafka Configuration
        │
        ├── Bootstrap Server
        ├── Telemetry Topic
        └── Producer Client ID
        │
        ▼
Kafka Administration
        │
        ▼
Kafka AdminClient
        │
        ▼
Apache Kafka
```

### Testing

The existing backend test suite was executed after the Kafka configuration and administration changes.

Test result:

```text
4 passed
```

The GitHub Actions workflow was also configured to run the test suite using:

```text
python -m pytest
```

with:

```text
PYTHONPATH=.
```

This ensures that the backend package can be imported correctly in the CI environment.

### Backend Components Completed

* [x] Centralized Kafka configuration
* [x] Environment-based Kafka settings
* [x] Kafka telemetry topic definition
* [x] Kafka producer client ID configuration
* [x] Kafka AdminClient utility
* [x] Kafka broker availability check
* [x] Local Kafka connectivity verification
* [x] Automated test execution

### Git

Commit:

```text
feat: add Kafka configuration and administration
```

Commit:

```text
e90e1e1
```

### Day 3 Result

The StreamForge backend now has a centralized Kafka configuration and administration layer. Kafka connection details and topic definitions are reusable across backend components, while the administration utility provides a simple way to verify Kafka broker availability.

---

## Day 4 — Backend: Kafka Telemetry Producer

### Overview

Day 4 focused on implementing the **Kafka telemetry producer** for the StreamForge backend.

The producer is responsible for taking validated `Telemetry` objects, converting them into JSON, and publishing them to the `truck-telemetry` Kafka topic.

### Backend Objectives

* Create a reusable Kafka telemetry producer.
* Connect the backend producer to the Kafka broker.
* Use the configured telemetry topic.
* Convert Pydantic telemetry objects into JSON.
* Use `truck_id` as the Kafka message key.
* Publish telemetry events asynchronously to Kafka.
* Handle Kafka message delivery results.
* Provide a flush operation for pending messages.
* Add automated producer tests.
* Verify actual message delivery using the local Kafka broker.

### Producer Location

The Kafka producer was created in:

```text
backend/
└── kafka/
    └── producer.py
```

### Telemetry Producer

The `TelemetryProducer` class was introduced to publish truck telemetry events.

```python
class TelemetryProducer:
    """Publish truck telemetry events to Kafka."""
```

The producer uses the existing Kafka configuration:

```python
from backend.kafka.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_PRODUCER_CLIENT_ID,
)
```

and the configured telemetry topic:

```python
from backend.kafka.topics import TRUCK_TELEMETRY_TOPIC
```

### Kafka Producer Configuration

The producer is initialized using:

```python
Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": KAFKA_PRODUCER_CLIENT_ID,
    }
)
```

The producer therefore uses the centralized Kafka configuration created during the previous backend stage.

### Publishing Telemetry

The producer accepts a validated `Telemetry` object:

```python
def publish(self, telemetry: Telemetry) -> None:
```

The telemetry object is converted into JSON:

```python
payload = telemetry.model_dump_json()
```

The message is then published to Kafka:

```python
self.producer.produce(
    topic=TRUCK_TELEMETRY_TOPIC,
    key=telemetry.truck_id,
    value=payload,
    callback=self._delivery_report,
)
```

The `truck_id` is used as the Kafka message key.

### Delivery Reporting

A delivery callback was implemented to report whether a Kafka message was successfully delivered.

Successful delivery reports:

```text
Kafka message delivered:
topic=truck-telemetry
partition=<partition>
offset=<offset>
```

If delivery fails, the producer reports the Kafka error.

### Flush Operation

A `flush()` method was added to wait for pending Kafka messages:

```python
def flush(self) -> None:
    """Wait for pending Kafka messages to be delivered."""
    self.producer.flush()
```

This is useful when the application needs to ensure that queued messages have been delivered before terminating.

### Backend Data Flow

```text
Truck Telemetry
       │
       ▼
Telemetry Pydantic Model
       │
       ▼
TelemetryProducer
       │
       ├── Convert to JSON
       ├── Use truck_id as key
       └── Publish message
       │
       ▼
Apache Kafka
       │
       ▼
truck-telemetry
```

### Producer Tests

Producer tests were added in:

```text
tests/
└── test_kafka_producer.py
```

The tests verify:

* Kafka producer creation
* Telemetry event creation
* Telemetry publishing
* Kafka producer flush operation

### Local Kafka Verification

The local Kafka broker was started using:

```powershell
docker compose up -d
```

Kafka connectivity was verified on:

```text
localhost:9092
```

The `truck-telemetry` topic was confirmed to be available.

### Message Delivery Verification

A telemetry event was published using:

```python
Telemetry(
    truck_id="TRUCK-000001",
    temperature=32.5,
)
```

The producer successfully reported:

```text
Kafka message delivered:
topic=truck-telemetry
partition=3
offset=1
```

The Kafka console consumer was then used to verify the actual message:

```json
{
  "truck_id": "TRUCK-000001",
  "temperature": 32.5,
  "timestamp": "2026-08-19T08:16:24.493034Z"
}
```

This confirmed that the backend producer successfully published a valid telemetry event to Kafka.

### Test Result

The complete backend test suite passed:

```text
6 passed
```

The producer-specific tests passed successfully.

### Backend Components Completed

* [x] Kafka telemetry producer
* [x] Kafka producer configuration
* [x] Telemetry JSON serialization
* [x] `truck_id` message key
* [x] Kafka delivery callback
* [x] Producer flush operation
* [x] Kafka producer tests
* [x] Actual Kafka message delivery verification
* [ ] Telemetry generator
* [ ] Benchmarking
* [ ] Kafka consumer
* [ ] Telemetry processing pipeline
* [ ] API layer
* [ ] Database/storage integration

### Git

Commit:

```text
feat: add Kafka telemetry producer
```

Commit:

```text
fdbde17
```

### Day 4 Result

The StreamForge backend can now **publish validated truck telemetry events to Kafka**.

The telemetry producer successfully converts Pydantic telemetry objects into JSON and delivers them to the `truck-telemetry` Kafka topic, completing the producer side of the initial streaming pipeline.

```
```

Yes. **Day 5 should be the telemetry generator + benchmarking stage**, based on the actual work already completed in your repository.

Since you want the README to have **one documentation commit per day**, add the following **after Day 4** in `README.md`.

````markdown
---

## Day 5 — Backend: Telemetry Generator and Benchmarking

### Overview

Day 5 focused on building the **telemetry generation and benchmarking layer** for the StreamForge backend.

The purpose of this stage was to generate realistic truck temperature telemetry events and provide a reusable way to publish multiple events through the Kafka producer.

A benchmarking utility was also added to measure telemetry publishing performance.

### Backend Objectives

- Create a reusable telemetry generator.
- Generate telemetry events for multiple trucks.
- Generate realistic temperature values.
- Reuse the existing Pydantic `Telemetry` model.
- Connect the telemetry generator with the Kafka producer.
- Support configurable numbers of telemetry events.
- Add automated tests for telemetry generation.
- Create a benchmarking utility.
- Measure telemetry publishing performance.
- Verify the generated events can be published to Kafka.

### Backend Components Added

The following backend components were added during Day 5:

```text
backend/
├── producers/
│   ├── telemetry_generator.py
│   └── benchmark.py
│
└── kafka/
    └── producer.py
````

Tests were added in:

```text
tests/
└── test_telemetry_generator.py
```

### Telemetry Generator

The telemetry generator provides a reusable mechanism for creating truck telemetry events.

The generator uses the existing `Telemetry` Pydantic model:

```text
Telemetry
    │
    ├── truck_id
    ├── temperature
    └── timestamp
```

Generated telemetry events follow the same validated structure used by the Kafka producer.

### Telemetry Generation Flow

```text
Truck IDs
    │
    ▼
Telemetry Generator
    │
    ├── Generate truck ID
    ├── Generate temperature
    └── Generate timestamp
    │
    ▼
Telemetry Model
    │
    ▼
Validated Telemetry Event
    │
    ▼
Kafka Producer
    │
    ▼
truck-telemetry
```

### Multiple Truck Support

The generator was designed to support telemetry generation for multiple trucks.

Example truck identifiers:

```text
TRUCK-000001
TRUCK-000002
TRUCK-000003
TRUCK-000004
...
```

This allows the StreamForge backend to simulate telemetry arriving from a fleet of trucks instead of a single vehicle.

### Temperature Generation

The generator creates temperature telemetry values that can be used to simulate real-time truck temperature measurements.

Each generated event contains:

```text
truck_id
temperature
timestamp
```

Example:

```json
{
  "truck_id": "TRUCK-000001",
  "temperature": 32.5,
  "timestamp": "2026-08-20T10:00:00Z"
}
```

### Integration with Kafka Producer

The telemetry generator works together with the Kafka producer implemented during Day 4.

The overall backend pipeline is:

```text
Telemetry Generator
        │
        ▼
Telemetry Pydantic Model
        │
        ▼
TelemetryProducer
        │
        ▼
Apache Kafka
        │
        ▼
truck-telemetry
```

This creates the first complete telemetry generation and publishing workflow in StreamForge.

### Benchmarking

A benchmarking utility was created in:

```text
backend/
└── producers/
    └── benchmark.py
```

The benchmark is intended to measure the performance of telemetry publishing.

The benchmarking process measures the time required to generate and publish telemetry events through the Kafka producer.

### Benchmark Flow

```text
Start Benchmark
       │
       ▼
Generate Telemetry Events
       │
       ▼
Publish Events to Kafka
       │
       ▼
Flush Producer
       │
       ▼
Calculate Publishing Time
       │
       ▼
Benchmark Result
```

### Testing

Telemetry generator tests were added in:

```text
tests/
└── test_telemetry_generator.py
```

The tests verify the telemetry generation functionality and ensure that generated telemetry objects contain the expected structure.

### Test Result

The complete backend test suite was executed after the Day 5 changes.

```text
6 passed
```

The telemetry generator tests passed successfully.

### Backend Components Completed

* [x] Kafka infrastructure
* [x] Kafka configuration
* [x] Kafka administration
* [x] Telemetry Pydantic model
* [x] Kafka telemetry producer
* [x] Telemetry generator
* [x] Multiple truck telemetry generation
* [x] Temperature telemetry generation
* [x] Telemetry generator tests
* [x] Kafka publishing integration
* [x] Benchmarking utility
* [ ] Kafka consumer
* [ ] Telemetry processing pipeline
* [ ] API layer
* [ ] Database/storage integration
* [ ] Monitoring and observability
* [ ] Production deployment

### Backend Progress

At the end of Day 5, the backend streaming flow had progressed to:

```text
Truck Telemetry Generator
          │
          ▼
   Pydantic Validation
          │
          ▼
    Kafka Producer
          │
          ▼
   Apache Kafka
          │
          ▼
 truck-telemetry Topic
          │
          ▼
    Kafka Consumer
```

The producer side of the streaming pipeline was therefore ready for the consumer and processing stages.

### Git

Implementation commit:

```text
feat: add telemetry generator and benchmark
```

Commit:

```text
3abc135
```

### Day 5 Result

The StreamForge backend can now **generate telemetry events for multiple trucks and publish them through the Kafka producer**.

A benchmarking utility was also introduced to evaluate telemetry publishing performance.

The project is now ready to proceed to the **Kafka consumer and telemetry processing stage**.

````

### Important

For the README, **don't create another file**. Continue using the same:

```text
README.md
````

So your documentation sequence remains:

```text
Day 0 → README
Day 1 → README
Day 2 → README
Day 3 → README
Day 4 → README
Day 5 → README
Day 6 → README
Day 7 → README
```

After you paste Day 5 into the README, run:

```powershell
git status
```

and send me the output. Then I'll give you the **single-cell Git commands** to commit and push Day 5.

Yes. Based on the work you actually completed, **Day 6 = Kafka Consumer & Telemetry Processing**.

Add this **after Day 5** in the same `README.md`:

````markdown
---

## Day 6 — Backend: Kafka Telemetry Consumer

### Overview

Day 6 focused on implementing the **Kafka consumer** for the StreamForge backend.

The consumer is responsible for reading telemetry messages from the `truck-telemetry` Kafka topic, decoding the JSON payload, validating the data using the Pydantic `Telemetry` model, and returning validated telemetry events to the backend processing layer.

### Backend Objectives

- Create a reusable Kafka telemetry consumer.
- Connect the consumer to the Kafka broker.
- Subscribe to the `truck-telemetry` topic.
- Configure Kafka consumer groups.
- Support configurable offset behavior.
- Poll Kafka for telemetry messages.
- Handle Kafka errors.
- Decode JSON telemetry payloads.
- Validate consumed telemetry using Pydantic.
- Provide a clean consumer shutdown mechanism.
- Add automated consumer tests.
- Verify producer-to-consumer message flow using the local Kafka broker.

### Consumer Components Added

The Kafka consumer was created in:

```text
backend/
└── kafka/
    └── consumer.py
````

The telemetry consumer service was implemented in:

```text
backend/
└── consumers/
    └── telemetry_consumer.py
```

Tests were added in:

```text
tests/
└── test_kafka_consumer.py
```

### Kafka Consumer

The `TelemetryConsumer` class was introduced to consume truck telemetry events from Kafka.

```python
class TelemetryConsumer:
    """Consume truck telemetry events from Kafka."""
```

The consumer uses the configured Kafka broker:

```python
from backend.kafka.config import KAFKA_BOOTSTRAP_SERVERS
```

and the configured telemetry topic:

```python
from backend.kafka.topics import TRUCK_TELEMETRY_TOPIC
```

### Consumer Configuration

The consumer was configured with:

```text
bootstrap.servers
group.id
auto.offset.reset
enable.auto.commit
```

The consumer group provides a logical identity for the telemetry-consuming service.

The default consumer group is:

```text
streamforge-telemetry-consumer
```

The consumer also supports configurable offset behavior:

```python
auto_offset_reset: str = "earliest"
```

This allows the consumer to start from the earliest available message when using a new consumer group.

### Topic Subscription

The consumer subscribes to the StreamForge telemetry topic:

```python
self.consumer.subscribe([TRUCK_TELEMETRY_TOPIC])
```

Therefore, the consumer listens to:

```text
truck-telemetry
```

### Consuming a Message

A `consume_one()` method was implemented:

```python
def consume_one(self, timeout: float = 5.0) -> Telemetry | None:
```

The method polls Kafka for a message.

If no message is available within the configured timeout, it returns:

```text
None
```

When a message is received, the consumer checks for Kafka errors before processing the payload.

### JSON Decoding

Kafka message values are received as bytes.

The consumer decodes the message using UTF-8:

```python
payload = json.loads(message.value().decode("utf-8"))
```

This converts the Kafka JSON payload into a Python dictionary.

### Pydantic Validation

After decoding the Kafka message, the payload is validated using the existing `Telemetry` model:

```python
return Telemetry.model_validate(payload)
```

This ensures that telemetry received from Kafka follows the same data structure defined during Day 2.

### Consumer Data Flow

```text
Apache Kafka
     │
     ▼
truck-telemetry Topic
     │
     ▼
Kafka Consumer
     │
     ▼
Poll Message
     │
     ▼
Decode JSON
     │
     ▼
Pydantic Validation
     │
     ▼
Telemetry Object
     │
     ▼
Backend Processing
```

### Consumer Service

A continuous telemetry consumer service was implemented in:

```text
backend/
└── consumers/
    └── telemetry_consumer.py
```

The service creates a telemetry consumer using:

```python
consumer = TelemetryConsumer(
    group_id="streamforge-telemetry-service"
)
```

It continuously polls Kafka for new telemetry events.

When a valid telemetry event is received, the service prints:

```text
Received telemetry:
truck=<truck_id>,
temperature=<temperature>,
timestamp=<timestamp>
```

### Graceful Shutdown

The consumer service handles `KeyboardInterrupt` so that the consumer can be stopped cleanly.

The consumer is closed in the `finally` block:

```python
consumer.close()
```

This ensures that Kafka consumer resources are released when the service stops.

### Consumer Test

A test was added in:

```text
tests/
└── test_kafka_consumer.py
```

The test verifies that the Kafka consumer can be created successfully.

Example:

```python
consumer = TelemetryConsumer(
    group_id="streamforge-test-consumer"
)

assert consumer is not None

consumer.close()
```

### Automated Test Result

The complete backend test suite was executed after the consumer implementation.

```text
9 passed
```

The test suite included:

```text
tests/test_api.py
tests/test_kafka_consumer.py
tests/test_kafka_producer.py
tests/test_producer.py
tests/test_telemetry_generator.py
```

### Producer-to-Consumer Verification

The Kafka producer was used to publish a telemetry event:

```text
truck_id: TRUCK-CONSUMER-001
temperature: 29.5
```

The producer confirmed successful delivery:

```text
Kafka message delivered:
topic=truck-telemetry
partition=0
offset=254
```

The Kafka console consumer confirmed the exact message:

```json
{
  "truck_id": "TRUCK-CONSUMER-001",
  "temperature": 29.5,
  "timestamp": "2026-08-21T16:29:59.313292Z"
}
```

This verified that telemetry events were successfully written to the Kafka topic.

### Consumer Verification

The StreamForge `TelemetryConsumer` was then used to consume telemetry from Kafka.

A consumed telemetry object was successfully returned:

```text
{
    'truck_id': 'TRUCK-000001',
    'temperature': 32.5,
    'timestamp': datetime(...)
}
```

This confirmed that the consumer could:

* Connect to Kafka.
* Subscribe to the telemetry topic.
* Poll messages.
* Decode JSON.
* Validate telemetry.
* Return a `Telemetry` object.

### Latest Message Verification

The consumer was also tested using:

```text
auto_offset_reset="latest"
```

The consumer initially waited for a new message:

```text
Consumer ready - waiting for new message...
No new message received
```

After a new telemetry event was published, the consumer successfully received:

```text
{
    'truck_id': 'TRUCK-DAY6-LATEST',
    'temperature': 26.4,
    'timestamp': datetime(...)
}
```

This confirmed that the consumer can wait for and process newly arriving Kafka telemetry events.

### Backend Streaming Architecture

At the end of Day 6, the backend streaming flow was:

```text
Telemetry Generator
        │
        ▼
Telemetry Pydantic Model
        │
        ▼
Kafka Producer
        │
        ▼
Apache Kafka
        │
        ▼
truck-telemetry
        │
        ▼
Kafka Consumer
        │
        ▼
JSON Decoding
        │
        ▼
Pydantic Validation
        │
        ▼
Validated Telemetry
        │
        ▼
Telemetry Consumer Service
```

### Backend Components Completed

* [x] Kafka infrastructure
* [x] Kafka configuration
* [x] Kafka administration
* [x] Telemetry Pydantic model
* [x] Kafka telemetry producer
* [x] Telemetry generator
* [x] Benchmarking utility
* [x] Kafka telemetry consumer
* [x] Consumer group configuration
* [x] Configurable offset behavior
* [x] JSON message decoding
* [x] Pydantic telemetry validation
* [x] Consumer service
* [x] Graceful consumer shutdown
* [x] Kafka consumer tests
* [x] Producer-to-consumer verification
* [ ] API layer
* [ ] Database/storage integration
* [ ] Monitoring and observability
* [ ] Production deployment

### Day 6 Test Result

The complete project test suite passed successfully:

```text
9 passed in 0.49s
```

The producer and consumer were also manually verified against the running local Kafka broker.

### Git

Implementation commit:

```text
feat: add Kafka telemetry consumer
```

Commit:

```text
a4f17d3
```

### Day 6 Result

The StreamForge backend now has a working **Kafka producer-to-consumer streaming path**.

Telemetry can be generated, validated, published to Kafka, consumed from the `truck-telemetry` topic, decoded from JSON, and validated again before being passed to the backend consumer service.

The project is now ready for the final **Day 7 integration, validation, and project completion stage**.

````

After pasting it into `README.md`, run:

```powershell
git status
````

Send me the output, and I'll give you the **single-cell Day 6 commit + push commands**.
---

# Day 7 — Final Backend Integration, Validation & Project Status

## Overview

Day 7 represents the final stage of the initial StreamForge backend implementation.

The main objective was to validate the complete telemetry streaming pipeline developed during Days 0–6 and document the current implementation status, test results, architecture, remaining work, and future improvements.

StreamForge now contains the core backend components required to generate, validate, publish, consume, and process truck telemetry events using Apache Kafka.

## Final Backend Objectives

- Validate the complete telemetry streaming pipeline.
- Verify communication between the telemetry producer and Kafka.
- Verify communication between Kafka and the telemetry consumer.
- Validate telemetry data using the Pydantic model.
- Confirm JSON serialization and deserialization.
- Execute the complete automated test suite.
- Verify the local Kafka infrastructure.
- Document the completed backend components.
- Identify remaining backend work.
- Define future improvements.

## Final Backend Architecture

The current StreamForge backend follows this architecture:

```text
                    StreamForge Backend
                           │
                           ▼
                  Telemetry Generator
                           │
                           ▼
                  Pydantic Telemetry
                       Data Model
                           │
                           ▼
                   Kafka Producer
                           │
                           ▼
                    Apache Kafka
                           │
                           ▼
                  truck-telemetry
                           │
                           ▼
                   Kafka Consumer
                           │
                           ▼
                    JSON Decoding
                           │
                           ▼
                  Pydantic Validation
                           │
                           ▼
              Telemetry Consumer Service
```

## End-to-End Telemetry Flow

The complete telemetry flow is:

### 1. Telemetry Generation

The telemetry generator creates truck telemetry events.

Example:

```text
truck_id: TRUCK-DAY6-LATEST
temperature: 26.4
```

### 2. Data Validation

The telemetry event is represented using the Pydantic `Telemetry` model.

The model provides:

- Truck ID validation
- Temperature validation
- Automatic UTC timestamp generation
- JSON serialization

### 3. Kafka Publishing

The `TelemetryProducer` converts the validated telemetry object into JSON and publishes it to:

```text
truck-telemetry
```

The truck ID is used as the Kafka message key.

### 4. Kafka Processing

Apache Kafka stores the telemetry event in the configured topic.

The topic currently contains:

```text
truck-telemetry
```

with four partitions.

### 5. Kafka Consumption

The `TelemetryConsumer` subscribes to the telemetry topic and polls Kafka for incoming messages.

### 6. JSON Decoding

The consumer converts the Kafka message from bytes into JSON data.

### 7. Pydantic Validation

The decoded JSON payload is validated again using the `Telemetry` model.

This provides a consistent data contract between the producer and consumer.

### 8. Backend Processing

The validated telemetry object is passed to the telemetry consumer service for backend processing.

## End-to-End Verification

The complete producer-to-Kafka-to-consumer flow was manually verified during the implementation.

A telemetry event was successfully published:

```text
truck_id: TRUCK-DAY6-LATEST
temperature: 26.4
```

Kafka reported successful message delivery:

```text
Kafka message delivered:
topic=truck-telemetry
partition=3
offset=261
```

The consumer was then able to receive the newly published event.

Example result:

```text
{
    'truck_id': 'TRUCK-DAY6-LATEST',
    'temperature': 26.4,
    'timestamp': datetime(...)
}
```

This confirms the basic end-to-end telemetry streaming path.

## Kafka Infrastructure Verification

The local Kafka infrastructure was verified using Docker Compose.

Kafka was started using:

```powershell
docker compose up -d
```

The Kafka container was verified using:

```powershell
docker compose ps
```

Kafka connectivity was verified on:

```text
localhost:9092
```

The connection test returned:

```text
TcpTestSucceeded : True
```

The Kafka topic was verified using:

```powershell
docker exec streamforge-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

The telemetry topic was available:

```text
truck-telemetry
```

## Automated Testing

The complete automated test suite was executed after the Day 6 implementation.

The final test suite contained:

```text
tests/test_api.py
tests/test_kafka_consumer.py
tests/test_kafka_producer.py
tests/test_producer.py
tests/test_telemetry_generator.py
```

Final test result:

```text
9 passed
```

This confirms that all currently implemented automated tests passed successfully in the local development environment.

## Backend Components Completed

### Infrastructure

- [x] Git repository initialized
- [x] Project directory structure created
- [x] Python virtual environment
- [x] Docker-based Kafka infrastructure
- [x] Local Kafka broker
- [x] Kafka connectivity verification

### Kafka

- [x] Kafka configuration
- [x] Kafka topic configuration
- [x] Kafka administration utility
- [x] Kafka broker availability check
- [x] `truck-telemetry` topic
- [x] Four Kafka partitions
- [x] Kafka producer
- [x] Kafka consumer
- [x] Consumer groups
- [x] Configurable offset behavior

### Telemetry

- [x] Pydantic telemetry model
- [x] Truck ID validation
- [x] Temperature field
- [x] Automatic UTC timestamp
- [x] JSON serialization
- [x] JSON deserialization
- [x] Telemetry generator
- [x] Telemetry validation

### Streaming

- [x] Telemetry generation
- [x] Telemetry publishing
- [x] Kafka message delivery verification
- [x] Kafka message consumption
- [x] Producer-to-consumer verification
- [x] Consumer service
- [x] Graceful consumer shutdown

### Testing

- [x] API tests
- [x] Telemetry model tests
- [x] Kafka producer tests
- [x] Telemetry generator tests
- [x] Kafka consumer tests
- [x] Full pytest execution
- [x] 9 automated tests passing

## Current Project Structure

The major backend components developed so far include:

```text
StreamForge/
│
├── .github/
│   └── workflows/
│       └── ci.yml
│
├── backend/
│   ├── consumers/
│   │   └── telemetry_consumer.py
│   │
│   ├── kafka/
│   │   ├── admin.py
│   │   ├── config.py
│   │   ├── consumer.py
│   │   ├── producer.py
│   │   └── topics.py
│   │
│   ├── models/
│   │   └── telemetry.py
│   │
│   └── producers/
│       ├── benchmark.py
│       └── telemetry_generator.py
│
├── tests/
│   ├── test_api.py
│   ├── test_kafka_consumer.py
│   ├── test_kafka_producer.py
│   ├── test_producer.py
│   └── test_telemetry_generator.py
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## CI Testing

A GitHub Actions workflow was added to automatically execute the backend test suite.

The CI workflow:

1. Checks out the repository.
2. Sets up Python.
3. Installs project dependencies.
4. Runs the pytest test suite.

The test command is:

```text
python -m pytest
```

The project also uses:

```text
PYTHONPATH=.
```

to ensure the backend package can be imported correctly during CI execution.

## Git Development History

The project development was organized into separate commits representing the project stages.

### Day 0

```text
chore: initialize StreamForge project structure and environment
76c7249
```

### Day 1

```text
feat: add local Kafka infrastructure
d18763d
```

### Day 2

```text
feat: add truck telemetry data model
49b2f81
```

### Day 3

```text
feat: add Kafka configuration and administration
e90e1e1
```

### Day 4

```text
feat: add Kafka telemetry producer
fdbde17
```

### Day 5

```text
feat: add telemetry generator and benchmark
3abc135
```

### Day 6

```text
feat: add Kafka telemetry consumer
a4f17d3
```

### Documentation — Day 0

```text
docs: add StreamForge Day 0 documentation
72a9748
```

### Documentation — Day 1

```text
docs: add StreamForge Day 1 backend documentation
e7c68b7
```

### Documentation — Day 2

```text
docs: add StreamForge Day 2 backend documentation
8b4bea8
```

### Documentation — Day 3

```text
docs: add StreamForge Day 3 backend documentation
3ecef53
```

### Documentation — Day 4

```text
docs: add StreamForge Day 4 backend documentation
306af70
```

### Documentation — Day 5

```text
docs: add StreamForge Day 5 backend documentation
0d2ed53
```

### Documentation — Day 6

```text
docs: add StreamForge Day 6 backend documentation
2ba60ed
```

## Final Day 7 Checklist

- [x] Project initialized
- [x] Backend structure established
- [x] Local Kafka infrastructure configured
- [x] Kafka broker verified
- [x] Telemetry topic created
- [x] Telemetry data model implemented
- [x] Telemetry validation implemented
- [x] Kafka configuration implemented
- [x] Kafka administration implemented
- [x] Kafka producer implemented
- [x] Telemetry generator implemented
- [x] Benchmark utility implemented
- [x] Kafka consumer implemented
- [x] Telemetry consumer service implemented
- [x] Producer-to-Kafka flow verified
- [x] Kafka-to-consumer flow verified
- [x] JSON serialization verified
- [x] JSON deserialization verified
- [x] Pydantic validation verified
- [x] Automated tests passing
- [x] CI workflow configured
- [x] Backend documentation completed
- [ ] API integration
- [ ] Persistent database/storage
- [ ] Monitoring and observability
- [ ] Production deployment
- [ ] Frontend integration
- [ ] Production-scale performance testing

## Remaining Work

The initial Kafka telemetry backend is functional, but the complete StreamForge platform still requires additional components.

### API Layer

The backend API layer needs to be expanded to expose telemetry and processing functionality to external clients.

### Database / Storage

Persistent storage needs to be integrated for storing telemetry history and processed truck data.

### Monitoring

Production monitoring should be added for:

- Kafka broker health
- Producer errors
- Consumer lag
- Message throughput
- Processing failures
- Application health

### Frontend Integration

A frontend interface can be integrated with the backend to visualize truck telemetry and system status.

### Production Deployment

The current Kafka infrastructure is intended for local development.

Production deployment will require:

- Production Kafka configuration
- Secure credentials
- Environment-specific configuration
- Monitoring
- Logging
- Scaling
- Deployment automation

## Future Improvements

Potential future improvements include:

- Real-time telemetry dashboards
- Persistent telemetry storage
- Consumer lag monitoring
- Multiple Kafka consumers
- Stream processing
- Alert generation
- Truck health monitoring
- Temperature anomaly detection
- Historical telemetry analysis
- REST API integration
- WebSocket-based live updates
- Frontend dashboards
- Cloud deployment
- Production observability
- Performance and load testing

## Final Project Status

The StreamForge project has successfully completed the initial backend streaming foundation.

The current implementation demonstrates a complete local telemetry streaming pipeline:

```text
Generate
   │
   ▼
Validate
   │
   ▼
Serialize
   │
   ▼
Produce
   │
   ▼
Kafka
   │
   ▼
Consume
   │
   ▼
Deserialize
   │
   ▼
Validate
   │
   ▼
Process
```

The backend is therefore ready for the next development phase involving API integration, persistent storage, monitoring, frontend integration, and production deployment.

## Day 7 Result

Day 7 completes the initial **StreamForge backend development cycle**.

The project now has a working foundation for real-time truck telemetry streaming using:

- Python
- Pydantic
- Apache Kafka
- Docker
- Confluent Kafka client
- Pytest
- GitHub Actions

The complete local telemetry pipeline has been implemented and verified, with the current automated test suite passing successfully.

---

# Initial 7-Day Backend Development Summary

| Day | Backend Work | Status |
|-----|--------------|--------|
| Day 0 | Project Initialization | Completed |
| Day 1 | Kafka Infrastructure | Completed |
| Day 2 | Telemetry Data Model | Completed |
| Day 3 | Kafka Configuration & Administration | Completed |
| Day 4 | Kafka Telemetry Producer | Completed |
| Day 5 | Telemetry Generator & Benchmark | Completed |
| Day 6 | Kafka Telemetry Consumer | Completed |
| Day 7 | Final Integration & Validation | Completed |

## Overall Status

**Initial backend streaming foundation: COMPLETED**

**Automated tests: 9 passed**

**Kafka producer-to-consumer flow: VERIFIED**

**Remaining platform work: API, storage, monitoring, frontend, and production deployment**
---

## Day 8 — Backend: Telemetry Stream Processing

### Overview

Day 8 focused on establishing the first **telemetry stream processing layer** for StreamForge.

The backend was extended with reusable processing components for filtering, transforming, and grouping truck telemetry events before further downstream processing.

### Backend Objectives

- Add telemetry temperature validation.
- Filter invalid telemetry events.
- Normalize telemetry temperature values.
- Process individual telemetry events.
- Process batches of telemetry events.
- Group telemetry events by truck.
- Group telemetry events into one-minute windows.
- Add automated tests for the streaming processing layer.
- Verify that the existing Kafka pipeline remains functional.

### Streaming Components

The following modules were implemented:

```text
backend/
└── streaming/
    ├── dataflow.py
    ├── filters.py
    ├── transformations.py
    └── windowing.py
Perfect. ✅ **Day 8 implementation is successfully committed and pushed.**

Your GitHub history now has:

```text
d597347 feat: add telemetry stream processing pipeline
a3c5ba6 docs: add StreamForge Day 7 backend documentation
32d9144 test: add end-to-end telemetry pipeline test
2ba60ed docs: add StreamForge Day 6 backend documentation
...
```

And:

```text
nothing to commit, working tree clean
```

So the **implementation commit is complete**.

### Next: Day 8 documentation commit

Now we need to add the **Day 8 documentation to `README.md`** as a **separate commit**.

Add this at the end of your README:

````markdown
---

## Day 8 — Backend: Telemetry Stream Processing

### Overview

Day 8 focused on establishing the first **telemetry stream processing layer** for StreamForge.

The backend was extended with reusable processing components for filtering, transforming, and grouping truck telemetry events before further downstream processing.

### Backend Objectives

- Add telemetry temperature validation.
- Filter invalid telemetry events.
- Normalize telemetry temperature values.
- Process individual telemetry events.
- Process batches of telemetry events.
- Group telemetry events by truck.
- Group telemetry events into one-minute windows.
- Add automated tests for the streaming processing layer.
- Verify that the existing Kafka pipeline remains functional.

### Streaming Components

The following modules were implemented:

```text
backend/
└── streaming/
    ├── dataflow.py
    ├── filters.py
    ├── transformations.py
    └── windowing.py
````

### Telemetry Filtering

The filtering layer validates the temperature value of each telemetry event.

The accepted temperature range is:

```text
-50.0 °C to 100.0 °C
```

Telemetry outside this range is rejected from further processing.

The filtering logic is implemented in:

```text
backend/streaming/filters.py
```

The main functions are:

```python
is_valid_temperature()
filter_telemetry()
```

### Telemetry Transformation

A transformation layer was added to normalize telemetry temperature values.

Temperature values are rounded to two decimal places.

Example:

```text
25.678
```

becomes:

```text
25.68
```

The transformation logic is implemented in:

```text
backend/streaming/transformations.py
```

The main functions are:

```python
normalize_temperature()
transform_telemetry()
```

### Telemetry Dataflow

A simple processing dataflow was introduced to process telemetry events through the filtering and transformation stages.

```text
Telemetry Input
       │
       ▼
Temperature Validation
       │
       ├── Invalid ──► Discard
       │
       ▼
Temperature Transformation
       │
       ▼
Processed Telemetry
```

The dataflow implementation is located in:

```text
backend/streaming/dataflow.py
```

The main functions are:

```python
process_telemetry()
process_batch()
```

### Batch Processing

The backend can process multiple telemetry events as a batch.

Valid events continue through the pipeline, while invalid events are removed.

Example:

```text
Input:

TRUCK-001 → 25.0 °C
TRUCK-002 → 150.0 °C

        │
        ▼

Processing

        │
        ▼

Output:

TRUCK-001 → 25.0 °C
```

### Telemetry Windowing

A basic windowing layer was added for grouping telemetry events.

The implementation supports:

* Grouping events by truck ID.
* Grouping events into one-minute time windows.

The windowing logic is implemented in:

```text
backend/streaming/windowing.py
```

### Grouping by Truck

Telemetry events can be grouped using the truck identifier:

```text
TRUCK-001
   ├── Telemetry Event 1
   └── Telemetry Event 2

TRUCK-002
   └── Telemetry Event 1
```

This provides the foundation for future per-truck stream analytics.

### One-Minute Windows

Telemetry timestamps are normalized to the beginning of their minute.

For example:

```text
12:30:10
12:30:45
```

are grouped into:

```text
12:30:00
```

This provides the foundation for future window-based telemetry analysis.

### Backend Processing Flow

```text
Truck Telemetry
       │
       ▼
Kafka Consumer
       │
       ▼
Telemetry Model
       │
       ▼
Filtering
       │
       ▼
Transformation
       │
       ▼
Windowing / Grouping
       │
       ▼
Processed Telemetry
```

### Tests

Day 8 introduced:

```text
tests/
└── test_streaming.py
```

The tests verify:

* Valid temperature detection.
* Invalid temperature detection.
* Invalid telemetry filtering.
* Temperature normalization.
* Single telemetry processing.
* Batch telemetry processing.
* Grouping telemetry by truck.
* Grouping telemetry by minute.

### Day 8 Test Result

The Day 8 streaming tests passed successfully:

```text
7 passed
```

The complete StreamForge test suite was also executed.

Final result:

```text
17 passed in 0.76s
```

This confirms that the new streaming processing layer did not break the existing Kafka, producer, consumer, telemetry model, generator, or integration functionality.

### Backend Components Completed

* [x] Kafka infrastructure
* [x] Telemetry data model
* [x] Kafka configuration
* [x] Kafka producer
* [x] Kafka consumer
* [x] Telemetry generator
* [x] End-to-end Kafka pipeline
* [x] Temperature filtering
* [x] Temperature transformation
* [x] Batch processing
* [x] Truck-based grouping
* [x] One-minute windowing
* [x] Streaming processing tests
* [ ] Persistent state processing
* [ ] RocksDB state integration
* [ ] Prometheus metrics integration
* [ ] Advanced stream processing
* [ ] Production deployment

### Git

Implementation commit:

```text
feat: add telemetry stream processing pipeline
```

Commit:

```text
d597347
```

### Day 8 Result

The StreamForge backend now contains a **testable telemetry stream processing foundation**.

Telemetry events can be validated, filtered, transformed, processed in batches, grouped by truck, and organized into one-minute windows.

This establishes the foundation for the next stages involving **state management, metrics, and more advanced stream processing**.

````

After pasting it, run:

```powershell
git status
git add README.md
git status
git commit -m "docs: add StreamForge Day 8 backend documentation"
git push origin main
git status
git log --oneline -10
````

This gives you the desired separation:

```text
Day 8 implementation
        ↓
d597347 feat: add telemetry stream processing pipeline

Day 8 documentation
        ↓
docs: add StreamForge Day 8 backend documentation
```
## Day 9 — Backend API and Kafka Consumer Integration

### Objective

Integrate the Kafka telemetry consumer with the backend API so that real truck telemetry events consumed from Kafka can be made available to the application.

### Work Completed

- Added the FastAPI backend application entry point.
- Added a backend health-check endpoint.
- Added a telemetry API route.
- Connected the Kafka telemetry consumer with the backend telemetry API.
- Updated the telemetry consumer to forward consumed Kafka events to the backend telemetry store.
- Enabled the backend to expose received telemetry data through an API endpoint.
- Prepared the backend API for integration with the frontend dashboard.

### Backend Components

- `backend/api/main.py`
- `backend/api/routes/health.py`
- `backend/api/routes/telemetry.py`
- `backend/consumers/telemetry_consumer.py`

### Data Flow

```text
Kafka
  ↓
Telemetry Consumer
  ↓
Consumed Telemetry Event
  ↓
Backend Telemetry Store
  ↓
FastAPI Telemetry API
  ↓
Frontend Dashboard
