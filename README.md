# Sim Lineage Pipeline

A production-grade data pipeline that ingests photonic circuit simulation outputs, streams them through Apache Kafka, stores versioned datasets in Delta Lake, and exposes a lineage API to trace any dataset back to the exact simulation run, tool version, and git commit that produced it.

Built as a portfolio project targeting data platform engineering roles in semiconductor and quantum computing environments.

---

## Why this project exists

In silicon and photonic chip development, a dataset without provenance is a liability. When a yield analysis surfaces unexpected results, engineers need to answer: *which simulation run produced this data, what tool version was used, and what version of the pipeline code was running at the time?*

This pipeline answers all three questions automatically — every record written to Delta Lake carries a `run_id`, `git_sha`, and `tool_version` captured at ingest. The lineage API makes that provenance queryable in milliseconds.

---

## Architecture

```
Simulation outputs (JSONL files)
        │
        ▼
┌─────────────────┐
│   File Watcher  │  watchdog monitors ./sim_outputs/
│   + Producer    │  enriches with run_id, git_sha, ingested_at
└────────┬────────┘
         │ Kafka topic: sim-outputs (3 partitions)
         ▼
┌─────────────────┐
│  Spark Structured│  reads stream every 15 seconds
│  Streaming       │  parses schema, transforms records
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌────────┐  ┌──────────────┐
│ Delta  │  │ SQLite        │
│ Lake   │  │ Metadata Store│
│(versioned)│  │run_id→version│
└────────┘  └──────┬───────┘
                   │
                   ▼
          ┌────────────────┐
          │  FastAPI        │
          │  Lineage API    │
          │  :8000          │
          └────────────────┘
```

---

## Tech stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Apache Kafka 7.6 (KRaft) | Message streaming |
| Processing | Apache Spark 3.5.1 | Structured streaming |
| Storage | Delta Lake 3.1.0 | Versioned datasets |
| Lineage index | SQLite | Fast provenance lookup |
| API | FastAPI + Uvicorn | Lineage query endpoints |
| Orchestration | Docker Compose | Local infrastructure |
| Language | Python 3.11 | All pipeline code |

---

## Project structure

```
sim-lineage-pipeline/
├── docker-compose.yml        # Kafka, Spark, Kafka UI
├── check_delta.py            # Delta Lake verification script
├── .gitignore
├── producer/
│   ├── producer.py           # File watcher + Kafka producer
│   ├── generate_data.py      # Fake simulation data generator
│   └── requirements.txt
├── consumer/
│   ├── consumer.py           # Spark Structured Streaming consumer
│   ├── metadata_store.py     # SQLite read/write functions
│   ├── requirements.txt
│   └── __init__.py
└── api/
    └── api.py                # FastAPI lineage endpoints
```

---

## Quickstart

### Prerequisites

- Docker Desktop
- Python 3.11
- Java 17 (`brew install openjdk@17`)
- Homebrew

### 1. Clone and set up environment

```bash
git clone https://github.com/masoodqq/sim-lineage-pipeline.git
cd sim-lineage-pipeline

python3.11 -m venv venv
source venv/bin/activate
pip install -r producer/requirements.txt
pip install -r consumer/requirements.txt
pip install fastapi uvicorn
```

### 2. Start infrastructure

```bash
docker compose up -d
```

Wait 15 seconds, then verify:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

All four containers should show `Up`:
- `sim-lineage-pipeline-kafka-1`
- `sim-lineage-pipeline-spark-master-1`
- `sim-lineage-pipeline-spark-worker-1`
- `sim-lineage-pipeline-kafka-ui-1`

### 3. Start the producer (Terminal 1)

```bash
source venv/bin/activate
unset SPARK_HOME && unset PYTHONPATH
python producer/producer.py
```

### 4. Start the data generator (Terminal 2)

```bash
source venv/bin/activate
python producer/generate_data.py
```

You will see new simulation files being generated every 10 seconds and the producer publishing them to Kafka.

### 5. Start the Spark consumer (Terminal 3)

```bash
source venv/bin/activate
unset SPARK_HOME && unset PYTHONPATH
python consumer/consumer.py
```

Every 15 seconds you will see batch output like:

```
--- Batch 0 | 5 records ---
  run_id=run_4d8e1a2f → delta_version=0 git_sha=a3f9c12
  run_id=run_9b3c1e7f → delta_version=0 git_sha=a3f9c12
```

### 6. Start the lineage API (Terminal 4)

```bash
source venv/bin/activate
uvicorn api.api:app --reload --port 8000
```

---

## Lineage API

Interactive docs available at **http://localhost:8000/docs**

### `GET /lineage/{run_id}`

Trace a simulation run to its Delta Lake version and git commit.

```bash
curl http://localhost:8000/lineage/run_4d8e1a2f
```

```json
{
  "run_id": "run_4d8e1a2f",
  "results": [
    {
      "run_id": "run_4d8e1a2f",
      "git_sha": "a3f9c12",
      "delta_version": 0,
      "row_count": 716,
      "processed_at": "2026-04-26 14:39:18.068165"
    }
  ]
}
```

### `GET /datasets/{delta_version}`

Find all simulation runs that contributed to a specific dataset version.

```bash
curl http://localhost:8000/datasets/0
```

```json
{
  "delta_version": 0,
  "results": [
    {
      "run_id": "run_4d8e1a2f",
      "git_sha": "a3f9c12",
      "delta_version": 0,
      "row_count": 716,
      "processed_at": "2026-04-26 14:39:18.068165"
    }
  ]
}
```

---

## Delta Lake time travel

Query any historical version of the dataset:

```python
# Read the dataset as it looked at version 0
df = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("./delta/sim_outputs")
```

View full version history:

```bash
python check_delta.py
```

---

## Provenance fields

Every record written to Delta Lake carries these fields added at ingest time by the producer:

| Field | Description |
|---|---|
| `run_id` | Unique ID per file ingestion batch |
| `git_sha` | Git commit hash of the producer code at ingest time |
| `ingested_at` | UTC timestamp when the record entered Kafka |
| `tool_version` | Version of the EDA tool that produced the simulation |

---

## Monitoring

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8090 |
| Spark Master UI | http://localhost:8080 |
| Spark Worker UI | http://localhost:8081 |
| Lineage API docs | http://localhost:8000/docs |

---

## Simulated data schema

The fake data generator mimics outputs from photonic circuit EDA tools:

```json
{
  "cell_name": "ring_modulator_v3",
  "tool": "SPICE",
  "tool_version": "18.2",
  "metric": "insertion_loss_db",
  "value": -2.4,
  "timestamp": "2026-04-22T10:32:00Z",
  "run_id": "run_4d8e1a2f",
  "git_sha": "a3f9c12",
  "ingested_at": "2026-04-22T10:32:00.253Z"
}
```

Supported cells: `ring_modulator_v3`, `mzi_switch_v2`, `grating_coupler_v5`

Supported tools: `SPICE`, `Lumerical`, `Cadence`

Supported metrics: `insertion_loss_db`, `extinction_ratio_db`, `bandwidth_ghz`

---

## What I learned

- Designing Kafka topics with appropriate partition counts for parallel consumption
- Using Spark Structured Streaming with `foreachBatch` for custom write logic
- Delta Lake versioning, ACID guarantees, and time travel queries
- Capturing data provenance at ingest time using git SHA and run IDs
- Building a FastAPI lineage service backed by SQLite for fast provenance lookup
- Managing Python environment conflicts between system and venv installations
- Containerizing a multi-service data platform with Docker Compose

---

## Relevance to semiconductor data platforms

This pipeline directly maps to challenges in photonic chip development:

- **EDA tool outputs** → simulated as JSONL files from SPICE, Lumerical, Cadence
- **Data traceability** → every record linked to `run_id` and `git_sha`
- **Versioned datasets** → Delta Lake time travel enables point-in-time analysis
- **Yield analysis readiness** → structured schema supports PPA and yield queries
- **On-prem + cloud ready** → Kafka and Delta Lake work in both environments
