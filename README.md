# IoT Devices → Confluent Platform → Flink - Live Pipeline Demo

This demo shows how to build a **live data pipeline** from IBM Db2 (IoT source) through Confluent Platform, with **two-stage Flink stream processing** (merge + aggregate), and a **1:3 fanout** to PostgreSQL, Redis, and a Python/React frontend. The pipeline is fully containerized and runs locally using Docker Compose.

## Architecture

![Architecture Diagram](./imgs/demo-diagram.png)

A **data generator** container seeds 10 IoT devices into Db2, then continuously inserts new sensor measurements (append-only) at 2 rows/second. Device metadata and measurements are stored in separate Db2 tables and streamed independently into Kafka. **Flink's first job** (merge) performs a LEFT JOIN to enrich measurements with device metadata, outputting to a unified stream. **Flink's second job** (average) calculates 15-second tumbling window averages per device/metric using the Window TVF API (with a 10-second late-event watermark) and writes results to a separate topic. Both merged and average data flow to all three sinks (PostgreSQL, Redis, and frontend). The **frontend** has three tabs: live device data table, line charts of the last 15 minutes of averages, and a real-time **data lineage graph** showing the full pipeline topology with throughput metrics.

## Prerequisites

- **Docker Desktop ≥ 4.x** - at least **8 GB RAM** allocated, **15 GB** free disk space
  ```bash
  brew install --cask docker
  ```
- **Apple Silicon (M1/M2/M3):** Rosetta 2 handles the `linux/amd64` Db2 image automatically - no extra configuration needed

## Quick Start

### 1. Start the stack

```bash
./start.sh
```

The first run pulls all images and builds the custom containers - allow **5~10 minutes**. Subsequent restarts are fast (~30 s) because data volumes are preserved.

`start.sh` waits for Schema Registry, Kafka Connect, Control Center, Flink, and pgAdmin to be healthy, then prints the service URLs.

### IBM Db2 (`localhost:50000`)

**Two-table schema with relationship:**

**Table 1: `DB2INST1.IOT_DEVICES`** — Device metadata (seeded once at startup)

| Column | Type | Notes |
|--------|------|-------|
| `device_identifier` | VARCHAR(50) | Primary key; e.g., `device-01` |
| `vendor_name` | VARCHAR(100) | Device vendor |
| `serial_number` | VARCHAR(100) | Device serial number |
| `created_timestamp` | TIMESTAMP | Device creation timestamp |

**Table 2: `DB2INST1.IOT_DEVICES_MEASUREMENTS`** — Sensor measurements (append-only stream)

| Column | Type | Notes |
|--------|------|-------|
| `device_identifier` | VARCHAR(50) | Foreign key to `IOT_DEVICES` |
| `temp` | DOUBLE | Sensor reading in °C (~5–35), stateful random walk |
| `hmdt` | DOUBLE | Sensor reading in % (~30–75), stateful random walk |
| `press` | DOUBLE | Sensor reading in hPa (~1000–1025), stateful random walk |
| `created_timestamp` | TIMESTAMP | Measurement timestamp (append-only) |

> **Canonical format normalization:** The Db2 source connector applies a `ReplaceField` SMT that renames these Db2-specific column names to a canonical format (`deviceID`, `vendor`, `serialNumber`, `temperature`, `humidity`, `pressure`, `updatedAt`) before publishing to Kafka. This decouples the Db2 schema from the rest of the pipeline — any future Db2 column renames only require updating the SMT mapping, with zero changes downstream (Flink, sinks, frontend).
>
> Both tables are **append-only** (commit log pattern) — DB2 never updates rows. Upsert semantics are applied by the Flink merge job (LEFT JOIN on device_identifier) and at the sink level (PostgreSQL and Redis always reflect the latest reading per device).

```
Database: testdb
User    : db2inst1
Password: db2inst1-pwd
JDBC URL: jdbc:db2://localhost:50000/testdb
```

Open a Db2 CLI session:
```bash
docker exec -it db2-luw su - db2inst1
```

```sql
db2 connect to testdb
db2 "SELECT * FROM DB2INST1.IOT_DEVICES"
db2 "SELECT * FROM DB2INST1.IOT_DEVICES_MEASUREMENTS"
```

Watch rows being inserted live (refreshes every second):
```bash
./watch-db2.sh
```

### 2. Deploy source connector

In a second terminal, once `start.sh` has printed the service URLs:

```bash
./deploy-source-connector.sh
```

This step waits for DB2 and both `IOT_DEVICES` tables to exist, pre-creates the compacted `iot_devices_db2` topic (for efficient device state storage), and deploys the source connector that streams both device and measurement data.

![Connectors running in Control Center](./imgs/kafka-connectors-created.png)

### 3. Deploy the Flink merge job

Once the source connector is running and populating both `iot_devices_db2` and `iot_devices_measurements_db2` topics:

```bash
./deploy-flink-merge.sh
```

This submits the merge job, which performs a LEFT JOIN of measurements with device metadata and writes the enriched stream to `iot_devices_merged`.

### 4. Deploy the Flink averaging job

Once the merge job is running and populating `iot_devices_merged`:

```bash
./deploy-flink-avg.sh
```

This submits the average job, which calculates 15-second tumbling window averages from the merged stream. Results are written to `iot_devices_avg` and propagate to all sinks automatically.

### 5. Deploy sink connectors

Once both Flink jobs are running:

```bash
./deploy-sink-connectors.sh
```

This deploys the PostgreSQL and Redis sink connectors, which consume the merged stream and averages and write them to the respective databases.

### 6. Watch the data flow

| Interface | URL | Credentials |
|---|---|---|
| **Live Dashboard** | http://localhost:5001 | - |
| **Control Center** | http://localhost:9021 | - |
| **pgAdmin** | http://localhost:5050 | admin@admin.org / admin |
| **Redis Commander** | http://localhost:8087 | - |
| **Kafka Connect API** | http://localhost:8083 | - |
| **Schema Registry** | http://localhost:8081 | - |
| **Prometheus** | http://localhost:9090 | - |
| **Flink UI** | http://localhost:9081 | - |

## Stopping

```bash
# Stop - preserves nothing (clean slate on next start)
./stop.sh
```

## Service Details

### Kafka Topics

Four topics are created by the pipeline:

1. **`iot_devices_db2`** — Device metadata from Db2 source connector (log-compacted, keyed by deviceID). Pre-created with `cleanup.policy=compact` in `deploy-source-connector.sh`.
2. **`iot_devices_measurements_db2`** — Sensor measurements from Db2 source connector (~2 inserts/sec, polled every 500 ms). Keyed by deviceID but NOT compacted.
3. **`iot_devices_merged`** — Enriched stream from Flink merge job (measurements + device metadata via LEFT JOIN)
4. **`iot_devices_avg`** — 15-second tumbling window averages from Flink average job (~1 update/15 sec per device)

![Topics in Control Center](./imgs/kafka-topics-created.png)

**`iot_devices_db2` — device metadata (log-compacted):**

![iot_devices_db2 topic messages](./imgs/kafka-topic-db2inst1.iot_devices-data.png)

**`iot_devices_measurements_db2` — sensor measurements:**

Raw measurements from the Db2 source connector (temperature, humidity, pressure for each device).

**`iot_devices_merged` — enriched measurements:**

Measurements enriched with device metadata (vendor, serial number) via the Flink merge job.

**`iot_devices_avg` — Flink window average messages:**

![iot_devices_avg topic messages](./imgs/kafka-topic-iot_devices_avg-data.png)

```bash
# List topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# Consume raw device records
docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server broker:29092 \
  --topic iot_devices_db2 \
  --from-beginning

# Consume average records
docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server broker:29092 \
  --topic iot_devices_avg \
  --from-beginning

# Check consumer group lag
docker exec broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group connect-postgres-iot-devices-sink \
  --describe

# Check all consumer groups
docker exec broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --list
```

### PostgreSQL (`localhost:5432`)

Two tables are created automatically by the sink connector:

- **`iot_devices`** — enriched device data with measurements (from `iot_devices_merged` topic, upserted per device)
- **`iot_devices_avg`** — 15-second window averages (from `iot_devices_avg` topic, upserted per device/window)

![pgAdmin data view](./imgs/pgadmin-postgres-data-view.png)

```
Database: postgres
User    : postgres
Password: postgres
```

```bash
docker exec -it postgres psql -U postgres -d postgres -c 'SELECT * FROM iot_devices LIMIT 10;'
docker exec -it postgres psql -U postgres -d postgres -c 'SELECT * FROM iot_devices_avg ORDER BY window_end DESC LIMIT 10;'
```

pgAdmin is pre-configured with a **Demo - PostgreSQL** server. Open http://localhost:5050 and expand the server tree — no manual setup required.

### Redis (`localhost:6379`)

Device records and averages are stored as RedisJSON. Both topics share one connector; device records are stored under keys matching the device ID (e.g., `device-01`).

![Redis Commander data view](./imgs/redis-commander-data-view.png)

```bash
# List all device keys
docker exec -it redis redis-cli KEYS "*device*"

# Read one device record
docker exec -it redis redis-cli JSON.GET device-01

# Total key count
docker exec -it redis redis-cli DBSIZE
```

Browse all keys visually at http://localhost:8087 (Redis Commander).

### Python/React Frontend (`localhost:5001`)

The frontend automatically consumes from both Kafka topics and displays live data.

**Devices Table** — raw device readings updated in real-time:

![Frontend devices table](./imgs/frontend-table.png)

**Average Charts** — 15-second rolling averages per metric, one line per device:

![Frontend average charts](./imgs/frontend-charts.png)

**Data Lineage** — live pipeline topology graph showing nodes (DB2 Source → Kafka Topics → Flink → Sinks) with real-time bytes/sec throughput and consumer lag per edge:

![Frontend data lineage](./imgs/frontend-data-lineage.png)

Data updates via WebSocket in real-time. Connection status and event counts shown in the status bar.

## Connector Management

Three connectors are deployed: `db2-source-connector`, `postgres-iot-devices-sink`, and `redis-iot-devices-sink`.

```bash
# Status of all connectors
curl -s http://localhost:8083/connectors?expand=status | python3 -m json.tool

# Status of a single connector
curl -s http://localhost:8083/connectors/db2-source-connector/status | python3 -m json.tool

# Restart a connector
curl -X POST http://localhost:8083/connectors/db2-source-connector/restart

# Re-deploy source connector (safe to re-run)
./deploy-source-connector.sh

# Re-deploy sink connectors (safe to re-run)
./deploy-sink-connectors.sh
```

## Flink Job Management

```bash
# Monitor Flink job logs
docker logs flink-jobmanager

# Deploy/restart the merge job (enriches measurements with device metadata)
./deploy-flink-merge.sh

# Deploy/restart the averaging job (computes 15-sec rolling averages)
./deploy-flink-avg.sh
```

### Merge Job (`deploy-flink-merge.sh`)
Performs a **LEFT JOIN** of measurements with device metadata on `deviceID`:
- Reads from `iot_devices_db2` (device metadata)
- Reads from `iot_devices_measurements_db2` (sensor measurements)
- Outputs to `iot_devices_merged` with enriched fields: `deviceID, vendor, serialNumber, temperature, humidity, pressure, updatedAt`

### Averaging Job (`deploy-flink-avg.sh`)
Computes **15-second tumbling window averages** per device/metric:
- Reads from `iot_devices_merged`
- Uses Window TVF API on the `updatedAt` watermark with **10-second late-event tolerance**
- Produces averages per device every 15 seconds
- Results written to `iot_devices_avg` via upsert-kafka with Avro encoding

> **Note:** Flink tracks its read position using its own state backend, not Kafka's `__consumer_offsets`. Consumer group lag shown in the Data Lineage tab is therefore not meaningful for Flink sources.

## Troubleshooting

**DB2 data generator shows "not ready yet" on startup** — normal. DB2 takes 15–30 s to resume from a preserved volume, and 3–5 min on a full reset. The generator loops patiently until the JDBC port is open and the `IOT_DEVICES` table exists.

**Source connector in FAILED state** — run `./deploy-connectors.sh` again; the script automatically waits for DB2 and retries failed connectors.

**pgAdmin "no password supplied" error** — `start.sh` seeds the password automatically. If you skip `start.sh` and start containers manually, run:
```bash
./start.sh   # re-seeds the pgAdmin password as part of startup
```

**Port conflict** — check which process owns the port:
```bash
lsof -i :50000   # DB2
lsof -i :9021    # Control Center
```

## Resources

- [Confluent JDBC Connector docs](https://docs.confluent.io/kafka-connectors/jdbc/current/index.html)
- [Redis Kafka Connector docs](https://redis.io/docs/latest/integrate/kafka/)
- [IBM Db2 Community Edition](https://www.ibm.com/docs/en/db2/11.5)
- [Apache Flink SQL docs](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/overview/)
- [Confluent Control Center](http://localhost:9021)
