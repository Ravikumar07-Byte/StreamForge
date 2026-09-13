# StreamForge

## Project Overview

**StreamForge** is a distributed real-time event processing platform designed to process high-volume truck telemetry data using **Apache Kafka** and **Bytewax**.

The system simulates telemetry generated from a large fleet of trucks and processes the incoming events through a scalable streaming pipeline. The platform is built around continuous event ingestion, real-time filtering, transformation, event-time windowing, aggregation, and late-event handling.

The project focuses on building a reliable foundation for distributed stream processing using Python-based technologies.

### Key Capabilities

- Real-time truck telemetry generation
- High-throughput Kafka event ingestion
- Partitioned Kafka-based event streaming
- Bytewax stream processing
- `Consume → Filter → Map` processing topology
- Five-minute event-time windowing
- Per-truck telemetry aggregation
- Average temperature calculation
- Event-time watermark processing
- Late-event detection and separation
- Automated testing and throughput benchmarking

## Problem Statement

Modern transportation systems generate large volumes of telemetry data continuously from connected vehicles. Processing this data efficiently requires a distributed streaming architecture capable of handling high event rates while maintaining correct event-time processing.

StreamForge addresses this requirement by providing a real-time telemetry processing pipeline in which truck events are produced to Kafka, consumed by the streaming layer, filtered and transformed, grouped into five-minute event-time windows, and aggregated for per-truck analysis.

The system also accounts for real-world streaming challenges such as late-arriving events, event-time ordering, processing performance, and future state recovery.

## Objectives

- Build a distributed telemetry streaming pipeline using Apache Kafka
- Generate realistic synthetic truck telemetry events
- Process continuous telemetry using Bytewax
- Implement a structured `Consume → Filter → Map` topology
- Apply event-time based processing
- Group telemetry into five-minute windows
- Calculate per-truck average temperature
- Handle late-arriving telemetry using watermarks and allowed lateness
- Validate the processing pipeline through automated tests
- Measure streaming performance against the required throughput target
- Provide a foundation for persistent state recovery and monitoring in later phases

## Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.11 | Backend and stream-processing development |
| Apache Kafka | Distributed telemetry event streaming |
| Bytewax | Real-time stream processing |
| Confluent Kafka | Python Kafka client integration |
| FastAPI | Backend API layer |
| Pydantic | Telemetry data validation |
| RocksDB / rocksdict | Planned persistent state management |
| Prometheus | Planned system monitoring and metrics |
| React / Vite | Dashboard and visualization |
| Docker | Local Kafka infrastructure |
| Pytest | Automated testing |
| GitHub Actions | Continuous integration |

## Project Scope

StreamForge is developed incrementally across multiple phases.

The initial phase establishes the Kafka-based telemetry infrastructure and producer-consumer pipeline. The stream-processing phase extends this foundation with Bytewax processing, event-time windows, aggregation, watermarking, and late-event handling.

Future phases extend the platform with persistent state, Kafka changelog recovery, fault tolerance, monitoring, and a real-time dashboard.

---

# System Architecture

## Overall Architecture

StreamForge follows a distributed real-time event-processing architecture designed to ingest, transport, process, aggregate, and analyze continuous truck telemetry data.

```text
Truck Telemetry Generator
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
   Bytewax Stream Processing
          │
          ├── Consume
          ├── Filter
          └── Map
          │
          ▼
    Event-Time Processing
          │
          ├── Event-Time Clock
          ├── Watermark
          └── Late-Event Handling
          │
          ▼
   Five-Minute Event-Time Window
          │
          ▼
   Per-Truck Aggregation
          │
          ▼
   Average Temperature
          │
          ▼
   Processed Stream Output
          │
          ▼
   Backend API (FastAPI)
          │
          ▼
   Frontend Dashboard (React / Vite)
```

The architecture separates telemetry generation, message streaming, stream processing, event-time handling, aggregation, and downstream integration. This separation makes it possible to test the streaming and processing layer independently from Kafka and application-level components.

**Telemetry Generation Layer** — produces synthetic truck telemetry events that simulate continuous sensor data from a large fleet of connected trucks. Each event contains a truck ID, temperature, and timestamp:

```json
{
  "truck_id": "TRUCK_001",
  "temperature": 72.5,
  "timestamp": "2026-09-12T10:15:30"
}
```

Synthetic data makes it possible to generate controlled workloads for local development, Kafka integration testing, stream-processing validation, window and late-event testing, end-to-end pipeline testing, and throughput benchmarking — without requiring physical truck sensors.

**Backend API Layer (FastAPI)** — sits between the stream-processing system and application-level services. It is kept separate from the core stream-processing logic so the processing topology can be tested independently:

```text
Stream Processing → Processed Results → Backend Services → FastAPI → API Endpoints
```

**Frontend Layer (React / Vite)** — provides the user-facing dashboard, giving visibility into telemetry, processing status, stream-processing flow, and (in later phases) performance and bottleneck information. The frontend communicates with backend services rather than implementing stream-processing logic directly.

### Architecture Design Principles

- **Separation of Concerns** — `Generation → Streaming → Processing → Aggregation → API → Dashboard`, reducing coupling between components.
- **Event-Driven Processing** — telemetry is processed as a continuous stream of events rather than as a batch dataset.
- **Partition-Based Parallelism** — Kafka partitions provide the foundation for distributing event-processing workloads.
- **Event-Time Correctness** — window assignment is based on telemetry event timestamps, not arrival time.
- **Late-Event Awareness** — watermarks and allowed lateness provide controlled handling of delayed events.
- **Independent Processing Validation** — the Bytewax topology can be benchmarked independently of Kafka end-to-end overhead.

## Kafka Architecture

Apache Kafka is the event-streaming backbone of StreamForge, providing topics, partitions, producers, consumers, distributed event storage, and parallel event processing.

Current local development configuration:

```text
Kafka Container    : streamforge-kafka
Kafka Image        : apache/kafka:4.3.1
Kafka Port         : 9092
Topic              : truck-telemetry
Partitions         : 4
Replication Factor : 1
```

This configuration is intended for development, testing, and benchmarking. A production deployment could use multiple Kafka brokers and a higher replication factor to improve fault tolerance and availability.

**Topic and partitions** — the primary telemetry topic, `truck-telemetry`, is configured with four partitions so the event stream can be distributed and processed in parallel:

```text
truck-telemetry
       │
       ├── Partition 0
       ├── Partition 1
       ├── Partition 2
       └── Partition 3
```

**Producer** — publishes generated telemetry events to Kafka. It creates the telemetry event, serializes it, connects to the broker, and publishes to `truck-telemetry`, using the truck identifier as the logical event key where applicable.

**Consumer / stream input** — connects to Kafka, reads and deserializes telemetry events from `truck-telemetry`, and converts them into processing-ready events for the Bytewax dataflow:

```text
Apache Kafka → Telemetry Events → Stream Input → Bytewax Dataflow
```

**Scalability model** — additional partitions and processing workers can distribute the workload further:

```text
                         Kafka Topic
                              │
             ┌────────────────┼────────────────┐
             │                │                │
             ▼                ▼                ▼
        Partition 0      Partition 1      Partition 2
             │                │                │
             ▼                ▼                ▼
        Processor 0       Processor 1       Processor 2
             │                │                │
             └────────────────┼────────────────┘
                              │
                              ▼
                       Stream Results
```

The local four-partition configuration is primarily intended for development and validation, while the architecture is designed to support larger distributed workloads.

## Data Flow

The complete StreamForge data flow, end to end:

```text
Truck Telemetry Generator
        │
        ▼
    Kafka Producer
        │
        ▼
    Apache Kafka (truck-telemetry, 4 partitions)
        │
        ▼
  Bytewax Stream Input
        │
        ▼
      Consume
        │
        ▼
   Filter (Temperature > 0)
        │
        ▼
        Map
        │
        ▼
Event-Time Processing (Event-Time Clock + Watermark)
        │
    ┌───┴────┐
    ▼        ▼
On-Time   Late Events
Events        │
    │         ▼
    ▼     Late Handling
Five-Minute Window
    │
    ▼
Per-Truck Grouping (Truck ID + Window)
    │
    ▼
Aggregation (Average Temperature + Event Count)
    │
    ▼
Processed Output
    │
    ▼
Backend API (FastAPI)
    │
    ▼
React / Vite Dashboard
```

### Component Responsibilities

| Component | Responsibility |
|---|---|
| Telemetry Generator | Generates synthetic truck telemetry events |
| Kafka Producer | Publishes telemetry events to Kafka |
| Apache Kafka | Provides distributed event streaming |
| `truck-telemetry` Topic | Stores the truck telemetry event stream |
| Kafka Partitions | Provides event distribution and parallelism |
| Bytewax | Executes stream-processing operations |
| Consume | Receives events from the input stream |
| Filter | Removes invalid temperature events |
| Map | Transforms events for downstream processing |
| Event-Time Clock | Tracks event timestamps |
| Watermark | Tracks event-time progress |
| Late-Event Handling | Separates late-arriving events |
| TumblingWindower | Groups events into five-minute windows |
| Per-Truck Aggregation | Groups telemetry by truck and window |
| Average Aggregation | Calculates average temperature |
| FastAPI | Provides backend API integration |
| React / Vite | Provides dashboard visualization |
| Docker | Provides local infrastructure for Kafka |
| Pytest | Provides automated testing |
| GitHub Actions | Provides continuous integration |

### Current Architecture Status

The current implementation contains the core Week 1 and Week 2 streaming architecture: synthetic truck telemetry generation, a Kafka producer, Apache Kafka integration with the `truck-telemetry` topic (four partitions), Bytewax stream processing with the `Consume → Filter → Map` topology, temperature filtering (`Temperature > 0`), event-time processing with an event-time clock and watermark, 60-second allowed lateness, late-event handling, five-minute event-time windows, per-truck aggregation, average temperature calculation, event counting, and automated testing with throughput benchmarking.

This architecture is the foundation for the remaining development phases: persistent state and recovery, followed by monitoring and dashboard visualization.

---

# Project Setup

## Prerequisites

### Required Software

| Software | Purpose |
|---|---|
| Python 3.11 | Application and stream-processing development |
| Docker Desktop | Running Apache Kafka locally |
| Git | Source-code management |
| GitHub | Repository hosting and collaboration |
| Node.js | Required for the React / Vite frontend |
| npm | Frontend package management |

### Python Environment

StreamForge is developed and tested using Python 3.11. Verify the installed version:

```powershell
python --version
```

Expected output should be similar to:

```text
Python 3.11.x
```

## Installation

### Clone the Repository

```powershell
git clone https://github.com/Ravikumar07-Byte/StreamForge.git
cd StreamForge
git status
```

The repository contains the backend, streaming components, tests, infrastructure configuration, and frontend application.

### Create and Activate a Virtual Environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

After activation, the terminal should display the virtual-environment indicator:

```text
(.venv) PS C:\...\StreamForge>
```

### Install Python Dependencies

```powershell
pip install -r requirements.txt
```

Main dependencies include Bytewax, Confluent Kafka, FastAPI, Pydantic, rocksdict, Prometheus Client, and Pytest. Verify the installed environment with:

```powershell
pip list
```

### Project Structure

```text
StreamForge/
│
├── backend/
│   ├── producers/
│   ├── consumers/
│   ├── streaming/
│   └── state/
│
├── tests/
│
├── frontend/
│
├── requirements.txt
├── docker-compose.yml
└── README.md
```

The exact structure may evolve as additional project phases are implemented.

## Kafka Setup

Before starting Kafka, verify that Docker Desktop is running:

```powershell
docker --version
docker ps
```

### Starting Kafka

```powershell
docker compose up -d
docker ps
```

Confirm that the `streamforge-kafka` container is running. Kafka is exposed locally on `localhost:9092`.

### Verifying Kafka

```powershell
docker ps --filter "name=streamforge-kafka"
```

Kafka should be shown as a running container, available at `localhost:9092`. This broker is used by the telemetry producer and stream-processing components.

### Kafka Topic Setup

The primary topic is `truck-telemetry`, configured with four partitions:

```text
truck-telemetry
├── Partition 0
├── Partition 1
├── Partition 2
└── Partition 3
```

The topic can be created using the Kafka administration configuration included in the project. After creation, verify available topics using the Kafka CLI tools or the project's administration utilities.

## Running the Project

### Recommended Startup Order

| Step | Action |
|---|---|
| 1 | Start Docker Desktop |
| 2 | Start Kafka — `docker compose up -d` |
| 3 | Verify Kafka — `docker ps` (confirm `streamforge-kafka` is running) |
| 4 | Activate the Python environment — `.\.venv\Scripts\Activate.ps1` |
| 5 | Start the telemetry producer |
| 6 | Start Bytewax stream processing |
| 7 | Start the FastAPI backend |
| 8 | Start the frontend |

### Telemetry Producer

With Kafka running and the `truck-telemetry` topic available, activate the virtual environment and start the producer from the project's producer module or script. The producer generates synthetic truck telemetry and publishes it to `truck-telemetry`:

```text
Telemetry Generator → Kafka Producer → truck-telemetry → Apache Kafka
```

### Stream Processor

The Bytewax pipeline consumes telemetry from Kafka and processes it through the Week 2 topology:

```text
Consume → Filter → Map → Event-Time Processing → Five-Minute Window → Per-Truck Aggregation
```

Start it from the project's Bytewax pipeline module.

### Backend API

```powershell
uvicorn backend.app:app --reload
```

If the project's application module uses a different entry point, use the corresponding project command. FastAPI also provides interactive API documentation through its standard documentation endpoints when enabled.

### Frontend

```powershell
cd frontend
npm install
npm run dev
```

The terminal will display the local development URL provided by Vite. The frontend communicates with backend services and provides the dashboard interface.

### Complete Local Pipeline

```text
Docker / Kafka (streamforge-kafka, localhost:9092)
        │
        ▼
  Telemetry Producer
        │
        ▼
truck-telemetry Topic (4 Partitions)
        │
        ▼
  Bytewax Processor (Consume → Filter → Map)
        │
        ▼
Event-Time Windowing (Five-Minute Windows, Per-Truck Aggregation)
        │
        ▼
  Backend API (FastAPI)
        │
        ▼
  React / Vite Dashboard
```

### Verifying the Pipeline

```text
Telemetry Generated → Kafka Producer → Kafka Topic → Bytewax →
Filter → Map → Five-Minute Event-Time Window → Per-Truck Aggregation → Average Temperature
```

Successful data flow through this sequence confirms the main streaming components are communicating correctly.

### Running Tests

```powershell
pytest
```

### Stopping the Local Environment

```powershell
docker compose down
deactivate
```

### Setup Verification Checklist

- Python 3.11 environment is available
- Virtual environment can be activated
- Project dependencies are installed
- Docker Desktop is running
- Kafka container is running
- Kafka is available on port 9092
- `truck-telemetry` topic is available with four partitions
- Telemetry producer can publish events
- Bytewax can consume and process telemetry
- Five-minute event-time processing is available
- Automated tests execute successfully

---

# Week 1 — Kafka & Telemetry Foundation

## Week 1 Overview

Week 1 established the initial backend foundation of StreamForge: a reliable telemetry streaming pipeline built from project initialization and local infrastructure, through the telemetry data model, Kafka configuration, telemetry generation, Kafka producer, Kafka consumer, and finally end-to-end validation.

```text
Project Initialization
        │
        ▼
Local Kafka Infrastructure
        │
        ▼
Telemetry Data Model
        │
        ▼
Kafka Configuration
        │
        ▼
Kafka Producer
        │
        ▼
Telemetry Generator
        │
        ▼
Kafka Consumer
        │
        ▼
End-to-End Validation
```

By the end of Week 1, StreamForge had a functioning Kafka-based telemetry pipeline that could generate synthetic truck telemetry, publish it to Kafka, consume the events, and validate the complete producer-to-consumer flow.

### Week 1 Objectives

- Initialize the StreamForge project structure and Python environment
- Set up Apache Kafka locally using Docker and configure the broker
- Create the `truck-telemetry` topic with partitioned parallelism
- Define a structured truck telemetry data model
- Implement Kafka configuration and administration functionality
- Develop a reusable telemetry generator for multiple trucks
- Implement the Kafka telemetry producer and consumer
- Validate producer-to-Kafka and Kafka-to-consumer communication
- Perform end-to-end telemetry pipeline testing
- Establish the backend foundation required for Week 2 stream processing

## Day 0 — Project Initialization

**Scenario.** The project required a structured backend foundation before Kafka and telemetry processing could be implemented.

**Objective.** Create a clean project foundation where Kafka infrastructure, telemetry generation, backend processing, testing, and future stream-processing components could be developed independently.

**Process.**

```text
Create Project → Create Directory Structure → Configure Python Environment →
Prepare Dependencies → Prepare Testing Structure → Prepare CI Foundation
```

**Work completed.** The initial structure separated the major responsibilities of StreamForge:

```text
StreamForge/
│
├── backend/
│   ├── producers/
│   ├── consumers/
│   ├── streaming/
│   └── state/
│
├── tests/
├── frontend/
├── requirements.txt
├── docker-compose.yml
└── README.md
```

The Python environment was prepared for backend and streaming development, giving a foundation for backend development, Kafka integration, telemetry generation, stream processing, automated testing, continuous integration, and future state-management components.

**Why this was required.** A distributed streaming application contains multiple independent components. Separating them from the start makes the project easier to develop, test, maintain, extend, and debug.

**Result.** The basic StreamForge project foundation was successfully established — project structure ready, Python environment ready, development can begin.

## Day 1 — Local Kafka Infrastructure

**Scenario.** StreamForge requires a reliable event-streaming backbone capable of receiving continuous truck telemetry. Apache Kafka was selected as the messaging infrastructure.

**Objective.** Run Kafka locally and establish the infrastructure required for telemetry streaming.

**Process.** `StreamForge → Docker → Apache Kafka → Telemetry Event Stream`

**Work completed.** Apache Kafka was configured to run locally using Docker (`streamforge-kafka`, port `9092`). Kafka was selected because its topic and partition architecture provides a foundation for distributed event streaming and future parallel processing. The primary topic, `truck-telemetry`, was established with four partitions:

```text
truck-telemetry
│
├── Partition 0
├── Partition 1
├── Partition 2
└── Partition 3
```

**Why partitions were required.** Kafka partitions provide the foundation for distributing events across multiple processing paths instead of maintaining one sequential stream — important as the system scales to higher event volumes.

**Validation.** Starting the Docker Kafka environment, checking the container was running, verifying Kafka connectivity, and verifying the topic and partition configuration.

**Result.** The local Kafka infrastructure was successfully established with the `truck-telemetry` topic and its four partitions.

## Day 2 — Truck Telemetry Data Model

**Scenario.** Before telemetry could be sent through Kafka, StreamForge required a consistent structure for representing truck events, so different components would not interpret telemetry differently.

**Objective.** Create a structured telemetry model reusable by the generator, producer, consumer, and future stream-processing components.

**Telemetry structure.** Each event contains a Truck ID, Temperature, and Timestamp:

```json
{
  "truck_id": "TRUCK_001",
  "temperature": 27.5,
  "timestamp": "2026-09-12T10:15:30"
}
```

**Process.** `Telemetry Data → Telemetry Model → Validation → Kafka Producer → Kafka`

**Work completed.** A structured telemetry model was introduced for truck identification, temperature representation, event timestamp representation, consistent serialization/deserialization, and future event-time processing — providing a common contract between the generator, producer, Kafka, consumer, and future stream processing.

**Why this was required.** A consistent event structure is essential for a streaming system; the same telemetry format must be understood by the producer, Kafka, the consumer, the stream processor, and future aggregation/state-management components.

**Result.** The StreamForge telemetry event format (Truck ID, Temperature, Timestamp) was successfully defined and became the standard event structure for the Week 1 pipeline.

## Day 3 — Kafka Configuration and Administration

**Scenario.** With Kafka infrastructure and the telemetry model available, the application needed configuration and administration functionality to communicate with Kafka reliably.

**Objective.** Centralize Kafka connection settings and provide utilities for managing and verifying the telemetry topic.

**Process.** `StreamForge Configuration → Kafka Broker (localhost:9092) → truck-telemetry`

**Work completed.** Kafka configuration and administration functionality was added, covering connection configuration, topic management, topic verification, local environment validation, and partition verification.

**Why this was required.** Keeping Kafka configuration separate from application logic makes the system easier to configure, test, maintain, reuse, and move between development and future deployment environments.

**Result.** The StreamForge backend had a defined Kafka configuration and administration layer covering the `truck-telemetry` topic.

## Day 4 — Kafka Telemetry Producer

**Scenario.** Once Kafka was running and the telemetry structure was defined, StreamForge needed a component capable of publishing telemetry events into Kafka.

**Objective.** Connect telemetry generation with Kafka and create the first active event-producing stage of the system.

**Process.**

```text
Telemetry Event → Telemetry Model → Serialization → Kafka Producer → Apache Kafka → truck-telemetry
```

**Work completed.** The Kafka telemetry producer was implemented to create and serialize telemetry events, connect to the broker, publish messages to `truck-telemetry` (distributed across its four partitions), handle producer configuration, and support continuous telemetry generation.

**Validation.** The producer was validated by publishing telemetry events and confirming Kafka accepted the messages, demonstrating that the producer could communicate with the broker and publish to the expected topic.

**Result.** The telemetry producer successfully connected the telemetry generation layer to Kafka.

## Day 5 — Telemetry Generator and Benchmarking

**Scenario.** Testing a streaming platform manually with individual events is not sufficient — StreamForge needed to simulate continuous telemetry from multiple trucks and generate larger event volumes for functional and performance testing.

**Objective.** Create a reusable synthetic telemetry generator and establish an initial benchmarking capability.

**Work completed.** The generator supports synthetic truck telemetry generation across multiple truck identifiers, realistic temperature and timestamp generation, reusable and configurable event counts, Kafka producer integration, and both functional and performance testing.

**Why synthetic data was used.** The project is a streaming simulation and does not require physical trucks or sensor hardware during development. Synthetic telemetry makes it possible to create controlled workloads for Kafka, consumer, stream-processing, windowing, late-event, and end-to-end testing, as well as throughput benchmarking.

**Benchmarking.** An early benchmarking capability was introduced to measure the speed at which telemetry could be generated and published, providing a performance baseline for StreamForge's high-throughput requirements.

**Process.** `Generate Truck Event → Validate Telemetry → Serialize Event → Publish to Kafka → Measure Performance`

**Result.** StreamForge could now generate controlled volumes of synthetic truck telemetry for Kafka and performance testing.

## Day 6 — Kafka Telemetry Consumer

**Scenario.** After implementing the producer, StreamForge required the receiving side of the Kafka pipeline: reading telemetry events from Kafka and converting them back into usable application data.

**Objective.** Implement a Kafka consumer capable of receiving, decoding, validating, and processing telemetry events from `truck-telemetry`.

**Process.**

```text
Kafka → truck-telemetry → Kafka Consumer → JSON Decoding → Telemetry Validation → Validated Telemetry
```

**Work completed.** The consumer connects to Kafka, subscribes to the telemetry topic, receives messages, deserializes JSON data, validates received telemetry, and processes the resulting events.

**Why this was required.** The producer alone only establishes event ingestion. The consumer completes the basic streaming communication path by proving that published events can be successfully received and interpreted by another application component.

**Validation.** Confirmed that `Producer → Kafka → Consumer` was functioning correctly and that received messages matched the expected telemetry structure.

**Result.** The Kafka consumer successfully completed the receiving side of the Week 1 telemetry pipeline.

## Day 7 — Final Integration and End-to-End Validation

**Scenario.** The individual Week 1 components had been implemented separately. The final step was to verify that all components could work together as one continuous telemetry pipeline.

**Objective.** Perform final integration and validate the complete producer → Kafka → consumer workflow.

**Integration process.**

```text
Step 1  Start Docker
Step 2  Start Kafka
Step 3  Verify Kafka Broker
Step 4  Verify truck-telemetry Topic
Step 5  Start Telemetry Generator
Step 6  Publish Telemetry Using Producer
Step 7  Receive Events Using Consumer
Step 8  Validate Received Telemetry
Step 9  Run End-to-End Tests
```

**End-to-end validation** covered Kafka infrastructure availability, topic availability, producer connectivity, telemetry generation, message publishing and delivery, consumer connectivity and message reception, JSON decoding, telemetry validation, and producer-to-consumer communication.

**Result.** The Week 1 execution successfully demonstrated the complete flow:

```text
Synthetic Truck Telemetry → Kafka Producer → Apache Kafka →
truck-telemetry → Kafka Consumer → Validated Telemetry
```

confirming that the fundamental Kafka telemetry streaming pipeline was operational.

### Week 1 Component Summary

| Day | Component | Main Outcome |
|---|---|---|
| Day 0 | Project Initialization | StreamForge project foundation established |
| Day 1 | Local Kafka Infrastructure | Docker-based Kafka environment established |
| Day 2 | Telemetry Data Model | Structured truck telemetry defined |
| Day 3 | Kafka Configuration | Kafka connection and administration configured |
| Day 4 | Kafka Producer | Telemetry publishing to Kafka implemented |
| Day 5 | Telemetry Generator | Synthetic telemetry generation and benchmarking established |
| Day 6 | Kafka Consumer | Telemetry consumption and validation implemented |
| Day 7 | Final Integration | Complete producer → Kafka → consumer pipeline validated |

### Transition to Week 2

The Week 1 implementation established the event-ingestion foundation required for the next phase. The system was ready to move from basic Kafka producer-consumer communication to real-time stream processing:

```text
Kafka → Bytewax → Consume → Filter → Map → Event-Time Processing →
Five-Minute Windows → Per-Truck Aggregation → Watermarks → Late-Event Handling
```

---

# Week 2 — Stream Processing

## Week 2 Objectives

Week 2 focused on converting the Kafka-based telemetry pipeline into a real-time stream-processing system using Bytewax.

- Introduce Bytewax for distributed stream processing
- Build the telemetry processing topology: `Consume → Filter → Map`
- Consume telemetry events from Kafka and validate/process records
- Filter invalid temperature events and transform records for downstream use
- Introduce event-time processing with five-minute windows
- Group telemetry events by truck and calculate average temperature per window
- Introduce watermark-based processing and handle late-arriving telemetry
- Validate window aggregation correctness
- Perform automated testing
- Conduct the 100,000 events/sec throughput audit required for the Week 2 review

## Day 8 — Bytewax Stream Processing

**Scenario.** After completing the Kafka telemetry foundation in Week 1, the next requirement was to process telemetry continuously instead of simply producing and consuming Kafka messages. Week 2 introduced **Bytewax** as the stream-processing framework, with the Kafka telemetry stream as input to a processing topology.

**Objective.** Establish the basic Bytewax stream-processing pipeline:

```text
Kafka → Consume → Filter → Map → Processed Telemetry
```

### Consume → Filter → Map

**Consume** — reads telemetry events from `truck-telemetry`. Each Kafka record (Truck ID, Temperature, Timestamp) is decoded and converted into the internal telemetry representation used by the pipeline.

**Filter** — removes telemetry records that do not satisfy the required temperature condition:

```text
Temperature > 0  → Continue processing
Temperature ≤ 0  → Filtered out
```

This prevents invalid or unwanted temperature events from entering downstream aggregation.

**Map** — transforms the validated telemetry event into the representation required by downstream stream-processing stages, providing a clean event representation before event-time windowing and aggregation.

**Work completed.** Added Bytewax stream processing, connected Kafka telemetry input to the pipeline, implemented `Consume → Filter → Map`, added telemetry event transformation, prepared the stream for event-time window processing, and added automated validation.

**Result.** The Kafka telemetry stream was successfully converted into a Bytewax processing pipeline with the basic `Consume → Filter → Map` topology.

## Day 9 — Kafka and Stream Integration

**Scenario.** After creating the Bytewax processing topology, the next step was integrating the stream-processing pipeline with the existing Kafka infrastructure, ensuring producer-generated telemetry flows continuously through Kafka into Bytewax.

**End-to-end processing flow.**

```text
Telemetry Generator → Kafka Producer → Apache Kafka → truck-telemetry →
Bytewax Consume → Filter → Map → Event-Time Processing → Window Aggregation
```

**Kafka integration.** The pipeline consumes events from the existing `truck-telemetry` topic. Kafka continues to provide event transport, partitioning, distributed ingestion, and producer/consumer decoupling, while Bytewax provides the processing layer above it. The topic's multiple partitions allow events to be distributed and provide the foundation for parallel stream processing.

**Work completed.** Integrated Bytewax with Kafka telemetry input, verified telemetry could enter the stream-processing layer, preserved the existing Kafka topic and producer architecture, connected stream processing with downstream event-time processing, and verified the integrated pipeline with automated tests.

**Result.** Kafka became the persistent event-ingestion layer while Bytewax became the stream-processing layer — the system was ready for event-time windowing.

## Day 10 — Five-Minute Event-Time Windows

**Scenario.** The project requires telemetry events to be grouped into five-minute chunks based on event timestamp. A simple processing-time window is insufficient because telemetry may arrive later than its actual event timestamp, so Week 2 introduced event-time processing.

**Objective.** Process events according to their event timestamp, group them into five-minute windows independently per truck, and calculate the average temperature within each window.

### Event-Time Processing

Every telemetry event contains a timestamp. Instead of using only the time at which the system receives the event, the pipeline uses the timestamp associated with the telemetry event itself:

```text
Telemetry Event (Truck ID, Temperature, Event Timestamp) → Event-Time Clock → Five-Minute Window
```

### Five-Minute Windowing

The implementation uses Bytewax event-time windowing with a **tumbling window** of 5 minutes. Events are assigned to windows according to their event timestamp:

```text
10:00:00 ───────── 10:05:00   Window 1
10:05:00 ───────── 10:10:00   Window 2
10:10:00 ───────── 10:15:00   Window 3
```

### Per-Truck Aggregation

Telemetry is aggregated independently for each truck — the stream is keyed by truck identifier so events from different trucks are never mixed:

```text
Truck-001 → Event, Event, Event → Five-Minute Average
Truck-002 → Event, Event        → Five-Minute Average
Truck-003 → Event, Event, Event → Five-Minute Average
```

### Average Temperature

For each truck and five-minute window, the average temperature is calculated:

```text
Temperatures: 40.0, 42.0, 41.0, 43.0
Average = (40 + 42 + 41 + 43) / 4 = 41.5
```

**Bytewax components used** include `EventClock`, `TumblingWindower`, window alignment, window folding/aggregation, and keyed stream processing, with the window length configured for five minutes.

**Work completed.** Added event-time processing, five-minute tumbling windows, event timestamp-based window assignment, per-truck grouping, five-minute temperature aggregation, average temperature calculation, and integrated window processing into the Bytewax pipeline.

**Result.** Telemetry events are now processed according to event time and aggregated into five-minute windows for each truck.

## Day 11 — Watermarks and Late Events

**Scenario.** In a real-time distributed system, telemetry events may not arrive in timestamp order — for example, an event timestamped 10:03:40 might arrive after events timestamped 10:04:30 and 10:04:50. This is a late-arriving event, requiring watermark-based event-time processing.

### Watermark

A watermark represents the progress of event time through the stream, helping the system determine when it has received enough events to finalize an event-time window:

```text
Event Time ──────────────────────────────────────►
10:00       10:05       10:10
              ↑
           Watermark
```

The watermark distinguishes events that belong to the current processing horizon from events that arrive after the expected event-time boundary.

### Allowed Lateness

The implementation uses an allowed lateness period of **60 seconds**, tolerating telemetry that arrives later than its event-time position:

```text
Event-Time Window
10:00 ───────── 10:05
                 │
                 ▼
        Allowed lateness up to 60 sec
```

### Late-Event Handling

```text
Incoming Event
      │
      ▼
Check Event Time
      │
      ├── On time ───────────► Window Aggregation
      ├── Within lateness ───► Late-aware processing
      └── Too late ──────────► Late Event Stream
```

Events arriving within the allowed lateness period are still processed according to event-time rules. Events outside the accepted window are separated as late events rather than corrupting normal aggregation — this maintains a dedicated, observable path for late data.

**Work completed.** Added event-time watermark handling, configured allowed lateness, added late-event detection and separation, integrated late-event handling with five-minute aggregation, and added tests for event-time and late-event behavior.

**Result.** The stream-processing pipeline can now handle out-of-order telemetry while maintaining event-time window semantics.

## Week 2 Processing Architecture

```text
                    Telemetry Producer
                               │
                               ▼
                    Apache Kafka (truck-telemetry)
                               │
                               ▼
                          Consume
                               │
                               ▼
                     Filter (Temperature > 0)
                               │
                               ▼
                    Map (Event Transformation)
                               │
                               ▼
                  Event Clock (Event-Time)
                               │
                               ▼
              Watermark / Allowed Lateness
                               │
                 ┌─────────────┴─────────────┐
                 ▼                           ▼
          5-Minute Window              Late Events
                 │
                 ▼
          Per-Truck Group
                 │
                 ▼
           Average Temperature
```

## Throughput Audit

The Week 2 Mid Review requires a throughput audit targeting **100,000 events/sec**. A dedicated benchmark was created for the audited processing topology (`Consume → Filter → Map`) to isolate core processing work from additional Kafka end-to-end transport overhead.

**Benchmark result** (100,000 events processed):

```text
Total events       : 100,000
Processing time    : 0.046 seconds
Processing rate    : 2,162,194.89 events/sec
```

```text
2,162,194.89 events/sec  >  100,000 events/sec target
```

**End-to-end Kafka result** (includes Kafka ingestion and system-level overhead):

```text
End-to-end time    : 2.567 seconds
End-to-end rate    : 38,958.60 events/sec
```

The end-to-end Kafka rate is reported separately and is not presented as the 100K/sec processing result. The Week 2 throughput audit is based on the dedicated audited processing topology.

**Conclusion.**

```text
Required target : 100,000 events/sec
Audited rate    : 2,162,194.89 events/sec
Result          : TARGET ACHIEVED   (≈21.6× the required target)
```

## Windowing Verification

Five-minute event-time windowing was validated using telemetry events with event timestamps, confirming that events are assigned using event time, windows are created correctly, events are grouped by truck, temperature values are aggregated independently, average temperature is calculated per truck/window, and late events are handled separately according to the configured lateness policy.

## Testing

The Week 2 implementation was validated using the project's automated test suite:

```text
38 passed in 2.57s
```

Coverage includes telemetry processing, Kafka integration, stream-processing behavior, event-time processing, five-minute windowing, aggregation behavior, late-event handling, and throughput benchmark validation.

## Week 2 Final Result

```text
Kafka → Consume → Filter → Map → Event-Time Processing → Watermark →
Five-Minute Window → Per-Truck Aggregation → Average Temperature
```

### Week 2 Completion Summary

| Area | Result |
|---|---|
| Stream Processing | Completed |
| Framework | Bytewax |
| Processing Topology | Consume → Filter → Map |
| Event-Time Processing | Completed |
| Window Type | Five-minute tumbling window |
| Aggregation | Per-truck average temperature |
| Watermark | Implemented |
| Allowed Lateness | 60 seconds |
| Late Events | Handled |
| Throughput Target | 100,000 events/sec |
| Audited Processing Rate | 2,162,194.89 events/sec |
| Automated Tests | 38 passed |
| Week 2 Status | Completed |

Week 2 provides the stream-processing foundation required before moving into Week 3, where persistent state, RocksDB state management, Kafka changelog integration, worker failure testing, partition rebalancing, and state recovery will be addressed.

---

# Current Implementation Status

StreamForge has completed the Kafka telemetry foundation and the Week 2 real-time stream-processing implementation. The project currently contains the core infrastructure required to ingest telemetry events from Kafka, process them using Bytewax, perform event-time windowing, aggregate telemetry per truck, and handle late-arriving events.

## Completed

**Week 1 — Kafka and Telemetry Foundation**

- Project structure and Python environment
- Continuous integration workflow
- Local Apache Kafka infrastructure and topic configuration
- Truck telemetry data model (Pydantic)
- Kafka configuration and administration utilities
- Kafka telemetry producer and synthetic telemetry generator
- Telemetry benchmarking utility
- Kafka telemetry consumer
- End-to-end producer-to-consumer validation

```text
Telemetry Generator → Pydantic Telemetry Model → Kafka Producer → Apache Kafka →
truck-telemetry → Kafka Consumer → JSON Decoding → Pydantic Validation → Validated Telemetry
```

**Week 2 — Stream Processing**

- Bytewax stream-processing integration and Kafka-to-Bytewax stream integration
- `Consume → Filter → Map` topology with temperature filtering and event transformation
- Event-time processing with five-minute tumbling windows
- Per-truck grouping and average temperature aggregation
- Event-time watermark processing with 60-second allowed lateness
- Late-event handling, windowing validation, throughput benchmarking, and automated testing

```text
Kafka → Consume → Filter (Temperature > 0) → Map → Event-Time Processing →
Watermark → Five-Minute Window → Per-Truck Aggregation → Average Temperature
```

**Current Kafka infrastructure**

```text
Kafka Container : streamforge-kafka
Kafka Image     : apache/kafka:4.3.1
Kafka Port      : 9092
Kafka Topic     : truck-telemetry
Partitions      : 4
Replication     : 1
```

**Current window processing** — telemetry is grouped into five-minute event-time windows keyed by truck identifier, so each truck has independent window calculations (e.g., Truck A and Truck B each maintain their own 10:00–10:05, 10:05–10:10, 10:10–10:15 averages, with no cross-truck mixing).

**Current late-event processing** — events are evaluated against a 60-second allowed lateness:

```text
Incoming Event → Event-Time Evaluation
   ├── On Time            → Window Processing
   ├── Within Allowed Lateness → Late-aware Processing
   └── Too Late            → Late Event Handling
```

**Throughput status**

```text
Required Target : 100,000 events/sec
Achieved Rate   : 2,162,194.89 events/sec
Status          : ACHIEVED   (≈21.6× target)

End-to-End Time : 2.567 seconds
End-to-End Rate : 38,958.60 events/sec
```

**Testing status**

```text
Total Tests : 38
Passed      : 38
Failed      : 0
Status      : PASS
```

### Current Project State

```text
Telemetry Generator → Kafka Producer → Apache Kafka (truck-telemetry) →
Bytewax → Consume → Filter (>0°C) → Map → Event Clock → Watermark →
Five-Minute Window → Group by Truck → Average Temperature
        │
        ├── Normal Results
        └── Late Events
```

### Implementation Status by Week

| Week | Focus | Status |
|---|---|---|
| Week 1 | Kafka & Telemetry Foundation | Completed |
| Week 2 | Stream Processing & Windowing | Completed |
| Week 3 | State & Recovery | In Progress |
| Week 4 | Monitoring & Dashboard | Planned |

## Remaining

**Week 3 preparation** — the current implementation provides the processing foundation required for Week 3, which will focus on persistent state and fault tolerance: RocksDB state management, persistent stream-processing state, Kafka changelog integration, worker failure testing, worker restart recovery, partition rebalancing, state restoration, and recovery validation. The objective is to ensure that a worker failure does not result in permanent loss of processing state.

**Week 4 preparation** — Week 4 will extend the platform with monitoring and visualization: Prometheus metrics (throughput, latency, errors, late events, worker health), stream-processing bottleneck identification, a React-based monitoring dashboard, and React Flow topology visualization.

```text
Week 1 (Completed) → Week 2 (Completed) → Week 3 (In Progress) → Week 4 (Planned)
```

StreamForge has successfully completed the core Kafka ingestion and Week 2 stream-processing requirements, and is now ready to move from basic stream processing toward persistent state management and fault-tolerant distributed processing.

---

# Testing & Validation

## Testing Overview

Testing was performed throughout the development of StreamForge to verify that each major component works correctly and that the complete telemetry processing pipeline behaves as expected — beginning with individual Week 1 components and continuing through the Week 2 stream-processing implementation.

Coverage includes: project and environment validation, telemetry model validation, Kafka infrastructure validation, producer and consumer validation, end-to-end Kafka validation, Bytewax stream-processing validation, event-time window validation, per-truck aggregation validation, late-event handling validation, throughput benchmarking, and full automated test-suite execution.

## Testing Strategy

```text
Unit Testing → Component Testing → Kafka Integration Testing →
Stream Processing Testing → Event-Time / Window Testing →
Throughput Testing → End-to-End Validation
```

This layered approach makes it possible to identify problems at the individual component level before validating the complete distributed pipeline.

## Week 1 Testing

- **Telemetry model** — verified that incoming telemetry contains the expected fields (Truck ID, Temperature, Timestamp) and follows the required structure; the Pydantic-based model validates structure before use by downstream components.
- **Kafka infrastructure** — the local Kafka container was started and checked via `docker compose up -d` / `docker compose ps`, with the broker confirmed accessible at `localhost:9092`.
- **Kafka topic** — `truck-telemetry` was verified with 4 partitions and a replication factor of 1.
- **Producer** — tested by generating telemetry and publishing it to Kafka. A successful delivery returns metadata such as:

  ```text
  Topic     : truck-telemetry
  Partition : 3
  Offset    : 261
  ```

- **Consumer** — verified to connect to Kafka, read telemetry records, decode JSON, validate telemetry fields, and produce validated telemetry objects.
- **End-to-end Kafka flow** — confirmed telemetry could travel successfully through the complete Week 1 pipeline:

  ```text
  Telemetry Generator → Pydantic Model → Kafka Producer → Kafka Broker →
  truck-telemetry → Kafka Consumer → JSON Decoding → Pydantic Validation
  ```

## Week 2 Testing

Week 2 testing focused on the Bytewax stream-processing layer: topology, filtering, mapping, event-time processing, five-minute windowing, per-truck aggregation, average temperature, watermarks, late events, and throughput.

- **Consume / Filter / Map** — Consume was verified to accept telemetry from the Kafka-based stream; the filter rule (`Temperature > 0`) was validated so qualifying events continue and non-qualifying events are removed; Map was verified to transform telemetry into the downstream representation.
- **Event-time testing** — validated that event placement into windows is based on the event's own timestamp rather than arrival order:

  ```text
  Telemetry Event → Read Event Timestamp → Event-Time Clock → Determine Window → Aggregate
  ```

- **Five-minute window testing** — verified that events are grouped into the correct five-minute intervals and that events from different intervals are not incorrectly combined.
- **Per-truck aggregation testing** — verified that events for different trucks (e.g., Truck A vs. Truck B) are aggregated independently, so one truck's telemetry cannot affect another's calculated result.
- **Average temperature testing** — validated using sample values `40, 42, 41, 43`, giving an expected average of `41.5`, confirmed against the processor's output.
- **Watermark testing** — validated that event timestamps are considered during processing, window progress is tracked, out-of-order events are handled per the configured policy, and late events do not disrupt normal processing.
- **Late-event testing** — validated with an out-of-order scenario (an event timestamped 10:03:40 arriving after events timestamped 10:04:30 and 10:04:50), confirming that the configured 60-second allowed lateness routes events correctly, with excessively late events separated into the late-event handling path.

## Throughput Testing

**Objective.** The Week 2 Mid Review requires a processing throughput target of **100,000 events/sec**. A dedicated benchmark was created to measure the core processing topology (`Consume → Filter → Map`).

**Benchmark configuration.** 100,000 total events, measuring execution time and resulting events-per-second rate.

**Result.**

```text
Total events       : 100,000
Processing time    : 0.046 seconds
Processing rate    : 2,162,194.89 events/sec

2,162,194.89 events/sec  >  100,000 events/sec
THROUGHPUT TARGET ACHIEVED
```

**End-to-end measurement** (includes Kafka transport and system overhead), reported separately:

```text
End-to-end time    : 2.567 seconds
End-to-end rate    : 38,958.60 events/sec
```

The dedicated Week 2 throughput audit uses the core processing rate of 2,162,194.89 events/sec for the audited topology.

## Full Automated Test Suite

```text
38 passed in 2.57s
```

| Test Result | Count |
|---|---|
| Passed | 38 |
| Failed | 0 |
| Total | 38 |

**Overall result: ALL TESTS PASSED**

## Validation Summary

| Validation Area | Result |
|---|---|
| Telemetry Model | Passed |
| Kafka Infrastructure | Passed |
| Kafka Topic | Passed |
| Producer | Passed |
| Consumer | Passed |
| End-to-End Kafka Flow | Passed |
| Bytewax Processing | Passed |
| Consume → Filter → Map | Passed |
| Event-Time Processing | Passed |
| Five-Minute Windows | Passed |
| Per-Truck Aggregation | Passed |
| Average Temperature | Passed |
| Watermark Processing | Passed |
| Late-Event Handling | Passed |
| Throughput Audit | Passed |
| Full Test Suite | 38 Passed |

## Testing Conclusion

Testing confirms that the current StreamForge implementation successfully performs the required Kafka ingestion and Bytewax stream-processing operations, validated from individual components through the complete processing pipeline:

```text
Final test result : 38 passed in 2.57s
Throughput target : 100,000 events/sec
Throughput achieved : 2,162,194.89 events/sec
Status : ACHIEVED
```

The validated system is ready to proceed to the next development stage involving persistent state management and fault recovery.

---

# Performance

## Performance Overview

Performance evaluation determined whether StreamForge can process telemetry at the throughput required by the project specification. The main Week 2 performance objective was to validate the stream-processing topology against a target of **100,000 events/sec**, evaluated at both the core stream-processing level and the complete Kafka-based execution level.

Evaluation covered: stream-processing throughput, processing execution time, number of processed events, Kafka end-to-end throughput, event-processing scalability, the multi-partition Kafka architecture, and throughput target comparison.

## Throughput Benchmark

A dedicated benchmark evaluated the core Bytewax processing topology (`Consume → Filter → Map`) against 100,000 events, measuring how quickly the pipeline could execute the required operations.

**Result.**

```text
Total events       : 100,000
Processing time    : 0.046 seconds
Processing rate    : 2,162,194.89 events/sec
```

```text
Required Target           : 100,000 events/sec
Achieved Processing Rate  : 2,162,194.89 events/sec
```

The audited stream-processing topology exceeded the required target.

## Throughput Target Analysis

```text
2,162,194.89 / 100,000 ≈ 21.6×
```

The measured processing throughput is approximately **21.6×** the required Week 2 target — **TARGET ACHIEVED**.

## End-to-End Kafka Performance

A separate measurement for the complete Kafka-based execution produced:

```text
End-to-end time    : 2.567 seconds
End-to-end rate    : 38,958.60 events/sec
```

This measurement includes Kafka communication, event ingestion, serialization/deserialization, network communication, consumer-side processing, and system-level execution overhead — hence the end-to-end Kafka throughput is lower than the isolated core processing throughput.

### Processing Throughput vs. End-to-End Throughput

| Measurement | Result |
|---|---|
| Required target | 100,000 events/sec |
| Core processing rate | 2,162,194.89 events/sec |
| End-to-end Kafka rate | 38,958.60 events/sec |

The 2.16M events/sec figure is the dedicated Week 2 processing audit result for the core `Consume → Filter → Map` topology. The 38,958.60 events/sec figure represents the complete Kafka-based execution, including transport and system overhead. These measurements are reported separately rather than treated as the same benchmark.

## Kafka Partitioning and Scalability

The `truck-telemetry` topic uses multiple partitions, providing the foundation for distributing telemetry events across processing capacity:

```text
                  Kafka
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
     Partition   Partition   Partition
          │         │         │
          └─────────┼─────────┘
                    ▼
               Stream Processing
```

This architecture allows the system to scale processing across multiple workers as the project moves toward the distributed state and recovery requirements of Week 3.

## Performance Validation

Performance was validated using a fixed workload of 100,000 events, measuring the number of events processed, total processing time, processing throughput, and comparison against the required target:

```text
100,000 events → 0.046 seconds → 2,162,194.89 events/sec →
Target: 100,000 events/sec → ACHIEVED
```

Performance testing was performed alongside functional testing; the complete automated test suite produced `38 passed in 2.57s`, confirming both functional correctness and performance capability were validated together.

## Performance Summary

| Metric | Value |
|---|---|
| Benchmark Events | 100,000 |
| Required Throughput | 100,000 events/sec |
| Core Processing Time | 0.046 sec |
| Core Processing Rate | 2,162,194.89 events/sec |
| Target Margin | ~21.6× |
| End-to-End Kafka Time | 2.567 sec |
| End-to-End Kafka Rate | 38,958.60 events/sec |
| Automated Tests | 38 passed |

## Performance Conclusion

The Week 2 performance audit confirms that the core Bytewax processing topology satisfies and significantly exceeds the required throughput target:

```text
Target   : 100,000 events/sec
Achieved : 2,162,194.89 events/sec
Status   : ACHIEVED
```

The separate Kafka end-to-end measurement of 38,958.60 events/sec is reported as an end-to-end system measurement because it includes Kafka and system overhead. The current architecture provides a strong performance foundation for the next development stage, with future performance work focused on distributed workers, Kafka partition utilization, persistent state management, recovery behavior, and monitoring overhead.

---

# Project Roadmap

## Roadmap Overview

StreamForge is developed incrementally across four weeks, each building on the previous implementation:

```text
Week 1 — Kafka & Telemetry Foundation
        ↓
Week 2 — Stream Processing
        ↓
Week 3 — State & Recovery
        ↓
Week 4 — Monitoring & Dashboard
```

The roadmap progresses from basic telemetry ingestion to distributed, fault-tolerant stream processing with monitoring and visualization.

### Week 1 — Kafka & Telemetry Foundation — `COMPLETED`

Established the infrastructure required to generate, transport, and consume truck telemetry: project structure, virtual environment, Docker-based Kafka infrastructure and topic, Pydantic telemetry model, Kafka producer, synthetic telemetry generator, Kafka consumer, and end-to-end telemetry validation.

### Week 2 — Stream Processing — `COMPLETED`

Introduced real-time stream processing using Bytewax: Kafka stream integration, `Consume → Filter → Map`, event-time processing, five-minute tumbling windows, per-truck aggregation, average temperature calculation, watermarks, allowed lateness, late-event handling, and throughput benchmarking.

```text
Required Target : 100,000 events/sec
Achieved Rate   : 2,162,194.89 events/sec
Status          : ACHIEVED

Testing Result  : 38 passed in 2.57s
```

## Week 3 — State & Recovery — `IN PROGRESS`

**Objective.** Extend StreamForge from stream processing into fault-tolerant distributed processing, ensuring processing state can survive worker failures and be restored correctly.

**Planned components:** RocksDB state management, persistent processing state, Kafka changelog integration, state snapshots, worker failure testing, worker restart handling, partition rebalancing, state recovery, and recovery validation.

**RocksDB state management** — RocksDB will be used as the persistent local state store for stream-processing state, supporting truck processing state, window aggregation state, metrics-related state, and recovery information:

```text
Bytewax Worker → Processing State → RocksDB → Persistent Local State
```

**Kafka changelog** — Kafka will act as the durable changelog mechanism so state can be reconstructed after failure:

```text
Stream Processing → State Update
                        ├── RocksDB (Local)
                        └── Kafka Changelog (Durable)
```

**Worker failure testing** — a worker failure scenario will be introduced deliberately to validate fault tolerance:

```text
Worker 1 (Processing, State Updates, Failure) → Recovery → State Restoration → Continue Processing
```

**Partition rebalancing** — when workers join or leave, Kafka partition ownership may change; the Week 3 validation will examine:

```text
Kafka Partitions → Worker Assignment → Worker Failure → Rebalancing →
New Worker Assignment → State Recovery → Processing Continues
```

**State recovery validation** — recovery testing will compare processing behavior before and after a simulated worker failure:

```text
Before Failure → State Exists → Worker Failure → Worker Restart →
State Restored → Processing Resumes
```

**Week 3 targets:**

| Requirement | Status |
|---|---|
| Persistent State | Required |
| Kafka Changelog | Required |
| Worker Failure Test | Required |
| Partition Rebalancing | Required |
| State Recovery | Required |

## Week 4 — Monitoring & Dashboard — `PLANNED`

**Objective.** Add observability and a visual monitoring interface, making stream-processing behavior visible and easier to monitor.

**Planned components:** Prometheus metrics, processing throughput and latency metrics, error metrics, late-event metrics, worker health metrics, React dashboard, React Flow topology visualization, and bottleneck visualization.

**Prometheus monitoring:**

```text
Stream Processing → Metrics Collection → Prometheus → Monitoring Data
```

Potential metrics: events processed, processing throughput, processing latency, error count, late-event count, worker status, and processing activity.

**Dashboard:**

```text
Prometheus Metrics → React Dashboard
        ├── Throughput
        ├── Latency
        ├── Errors
        ├── Worker Status
        └── Processing Topology
```

**React Flow topology visualization** will represent the processing stages (`Kafka → Consume → Filter → Map → Window → Aggregation`) and can be extended to show processing bottlenecks and system activity, associating runtime metrics with the corresponding processing stages.

## Overall Project Roadmap

```text
┌─────────────────────────────────────────────┐
│ WEEK 1 — Kafka & Telemetry Foundation        │
│ Generator → Producer → Kafka → Consumer      │
│ STATUS: COMPLETED                            │
└──────────────────────┬────────────────────────┘
                       ↓
┌─────────────────────────────────────────────┐
│ WEEK 2 — Stream Processing                   │
│ Consume → Filter → Map → Window → Aggregate  │
│ STATUS: COMPLETED                            │
└──────────────────────┬────────────────────────┘
                       ↓
┌─────────────────────────────────────────────┐
│ WEEK 3 — State & Recovery                    │
│ RocksDB → Changelog → Failure → Recovery     │
│ STATUS: IN PROGRESS                          │
└──────────────────────┬────────────────────────┘
                       ↓
┌─────────────────────────────────────────────┐
│ WEEK 4 — Monitoring & Dashboard              │
│ Prometheus → React → React Flow              │
│ STATUS: PLANNED                              │
└─────────────────────────────────────────────┘
```

### Roadmap Dependencies

- **Week 1 → Week 2** — the Kafka telemetry infrastructure provides the input stream required by Bytewax: `Kafka Foundation → Telemetry Stream → Bytewax Processing`.
- **Week 2 → Week 3** — the existing stream-processing pipeline provides the processing state that must be persisted and recovered: `Stream Processing → Window State → Persistent State → Recovery`.
- **Week 3 → Week 4** — the stateful, distributed processing system provides runtime information that can be exposed through monitoring: `Distributed Processing → Runtime Metrics → Prometheus → Dashboard`.

## Final Project Goal

The final StreamForge architecture is intended to provide high-throughput, real-time processing with event-time windowing, persistent state, fault recovery, monitoring, and visualization:

```text
                    StreamForge
                        │
                        ▼
               Telemetry Generator
                        │
                        ▼
                  Kafka Cluster
                        │
                        ▼
                Bytewax Processing
                        │
          ┌─────────────┴─────────────┐
          ▼                           ▼
   Event-Time Windows           Late Events
          │
          ▼
    Per-Truck State
          │
          ▼
        RocksDB
          │
          ▼
    Kafka Changelog
          │
          ▼
     Recovery Layer
          │
          ▼
   Prometheus Metrics
          │
          ▼
    React Dashboard
          │
          ▼
   React Flow Topology
```

The roadmap is designed to evolve StreamForge from a local Kafka telemetry pipeline into a distributed, fault-tolerant, observable real-time event-processing platform.

---

# Team / Contribution

## Team Overview

StreamForge is developed as a collaborative engineering project following a structured four-week development roadmap, with work divided into phases so each stage builds on the previous implementation:

```text
Week 1 → Kafka & Telemetry Foundation
Week 2 → Stream Processing
Week 3 → State & Recovery
Week 4 → Monitoring & Dashboard
```

## Contribution Model

Development follows a task-based contribution model. Each contributor works on assigned implementation areas and maintains their own development work through Git version control. The repository tracks source-code changes, test additions, infrastructure changes, documentation, performance benchmarks, bug fixes, and feature implementations. Each meaningful development task is represented through a separate Git commit.

## Development Contributions

**Kafka and infrastructure** — project structure, Python environment, CI workflow, Docker-based Kafka infrastructure, Kafka topic configuration, Kafka administration utilities.

**Telemetry** — truck telemetry data model, Pydantic validation, Kafka producer, synthetic telemetry generator, configurable event generation, telemetry benchmarking, Kafka consumer.

**Stream processing** — Bytewax integration, Kafka stream integration, `Consume → Filter → Map` topology, temperature filtering, event transformation, event-time processing, five-minute tumbling windows, per-truck aggregation, average temperature calculation, watermark processing, late-event handling, throughput benchmarking.

**Testing** — component-level validation, Kafka integration testing, end-to-end telemetry testing, stream-processing testing, event-time validation, windowing validation, late-event validation, throughput testing, and full automated test-suite execution (current result: `38 passed in 2.57s`).

**Performance** — the Week 2 performance audit processed 100,000 events and achieved 2,162,194.89 events/sec against a required target of 100,000 events/sec — **TARGET ACHIEVED**.

## Git Version Control

Git is used to maintain the project's development history, with commits categorized as:

| Prefix | Purpose |
|---|---|
| `feat` | New functionality |
| `test` | Testing and validation |
| `ci` | Continuous integration |
| `chore` | Project/infrastructure maintenance |
| `docs` | Documentation |
| `fix` | Bug fixes |

This provides a traceable history of the project's development:

```text
Task → Implementation → Testing → Git Commit → Repository History
```

## Development Progression

```text
Project Initialization → Kafka Infrastructure → Telemetry Model →
Producer → Telemetry Generator → Consumer → End-to-End Validation →
Bytewax Integration → Consume → Filter → Map → Event-Time Processing →
Five-Minute Windows → Watermarks → Late Events → Throughput Audit →
Week 2 Completion
```

Each new feature is built on top of a validated previous stage.

## Current Contribution Status

| Development Area | Status |
|---|---|
| Project Foundation | Completed |
| Kafka Infrastructure | Completed |
| Telemetry Model | Completed |
| Kafka Producer | Completed |
| Telemetry Generator | Completed |
| Kafka Consumer | Completed |
| End-to-End Pipeline | Completed |
| Bytewax Integration | Completed |
| Consume → Filter → Map | Completed |
| Event-Time Processing | Completed |
| Five-Minute Windows | Completed |
| Per-Truck Aggregation | Completed |
| Watermarks | Completed |
| Late-Event Handling | Completed |
| Throughput Audit | Completed |
| Automated Testing | Completed |
| Persistent State & Recovery | In Progress |
| Monitoring & Dashboard | Planned |

## Contribution and Quality Validation

```text
Automated Tests → 38 Passed
Performance Audit → 2.16M events/sec
Week 2 Target → 100K events/sec
Status → ACHIEVED
```

This provides evidence that the completed implementation is both functionally validated and performance tested.

## Future Contributions

**Week 3** — RocksDB persistent state, Kafka changelog, worker failure simulation, partition rebalancing, state recovery, recovery validation.

**Week 4** — Prometheus metrics, runtime monitoring, throughput and latency metrics, error and late-event metrics, React dashboard, React Flow topology, processing bottleneck visualization.

## Contribution Summary

The StreamForge project has progressed from a basic Kafka telemetry pipeline into a functioning event-time stream-processing system, with completed contribution areas covering:

```text
Kafka Infrastructure + Telemetry Generation + Producer / Consumer +
Bytewax Processing + Event-Time Windowing + Per-Truck Aggregation +
Late-Event Handling + Performance Validation + Automated Testing
```

The current implementation establishes the foundation for the remaining fault-tolerance and monitoring stages of StreamForge.
