# IoT Devices → Confluent Platform → Flink - Live Pipeline Demo

This demo shows how to build a **live data pipeline** from IBM Db2 (IoT source) through Confluent Platform, with **Flink stream processing** to calculate 1-minute rolling averages, and a **1:3 fanout** to PostgreSQL, Redis, and a Python/React frontend. The pipeline is fully containerized and runs locally using Docker Compose.

## Architecture

![Architecture Diagram](./imgs/demo-diagram.png)

A **data generator** container creates 10 IoT devices at startup, then continuously makes small incremental sensor value changes (~1 per second). Raw data flows through Confluent to all three sinks simultaneously. In parallel, **Flink** calculates 1-minute rolling averages per device/metric and writes results to a separate topic, which is also consumed by all three sinks. The **frontend** has two tabs: one shows the live device data table, the other shows line charts of the last 15 minutes of averages.

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

**Table: `DB2INST1.IOT_DEVICES`** — 10 IoT devices with sensor readings

| Column | Type | Notes |
|--------|------|-------|
| `deviceID` | VARCHAR(50) | Unique identifier, e.g., `device-01` |
| `vendor` | VARCHAR(100) | Manufacturer name |
| `serialNumber` | VARCHAR(100) | Device serial number |
| `temperature` | DOUBLE | Current reading in °C (~15–25) |
| `humidity` | DOUBLE | Current reading in % (~40–60) |
| `pressure` | DOUBLE | Current reading in hPa (~1000–1020) |
| `createdAt` | TIMESTAMP | When record was inserted |
| `updatedAt` | TIMESTAMP | When record was last updated |

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
db2 "SELECT COUNT(*) FROM DB2INST1.IOT_DEVICES"
```

Watch rows being updated live (refreshes every second):
```bash
./watch-db2.sh
```

### 2. Deploy connectors

In a second terminal, once `start.sh` has printed the service URLs:

```bash
./deploy-connectors.sh
```

This step waits for DB2 and the `IOT_DEVICES` table to exist, then deploys all three connectors (Db2 source + PostgreSQL sink + Redis sink) and verifies they reach `RUNNING` state.

![Connectors running in Control Center](./imgs/kafka-connectors-created.png)

### 3. Deploy the Flink averaging job

Once the source connector is running and populating the `DB2INST1.IOT_DEVICES` topic:

```bash
./deploy-flink-job.sh
```

This submits the Flink INSERT job, which starts calculating 1-minute tumbling window averages. Results are written to `IOT_DEVICES_AVG` and propagate to all sinks automatically.

### 4. Watch the data flow

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

Two topics are created automatically by the pipeline:

1. **`DB2INST1.IOT_DEVICES`** — Raw device data from Db2 source connector (~1 update/sec)
2. **`IOT_DEVICES_AVG`** — 1-minute tumbling window averages from Flink (~1 update/min per device)

![Topics in Control Center](./imgs/kafka-topics-created.png)

**`DB2INST1.IOT_DEVICES` — raw device messages:**

![DB2INST1.IOT_DEVICES topic messages](./imgs/kafka-topic-db2inst1.iot_devices-data.png)

**`IOT_DEVICES_AVG` — Flink window average messages:**

![IOT_DEVICES_AVG topic messages](./imgs/kafka-topic-iot_devices_avg-data.png)

```bash
# List topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# Consume raw device records
docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server broker:29092 \
  --topic DB2INST1.IOT_DEVICES \
  --from-beginning

# Consume average records
docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server broker:29092 \
  --topic IOT_DEVICES_AVG \
  --from-beginning

# Check consumer group lag
docker exec broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group connect-postgres-sink \
  --describe
```

### PostgreSQL (`localhost:5432`)

Two tables are created automatically by the sink connector:

- **`iot_devices`** — raw device data (from `DB2INST1.IOT_DEVICES` topic)
- **`iot_devices_avg`** — 1-minute window averages (from `IOT_DEVICES_AVG` topic)

![pgAdmin data view](./imgs/pgadmin-postgres-data-view.png)

```
Database: postgres
User    : postgres
Password: postgres
```

```bash
docker exec -it postgres psql -U postgres -d postgres -c 'SELECT * FROM iot_devices LIMIT 5;'
docker exec -it postgres psql -U postgres -d postgres -c 'SELECT * FROM iot_devices_avg ORDER BY window_end DESC LIMIT 5;'
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

**Average Charts** — 1-minute rolling averages per metric, one line per device:

![Frontend average charts](./imgs/frontend-charts.png)

Data updates via WebSocket in real-time. Connection status and event counts shown in the status bar.

## Connector Management

```bash
# Status of all connectors
curl -s http://localhost:8083/connectors?expand=status | python3 -m json.tool

# Status of a single connector
curl -s http://localhost:8083/connectors/db2-source-connector/status | python3 -m json.tool

# Restart a connector
curl -X POST http://localhost:8083/connectors/db2-source-connector/restart

# Re-deploy all connectors (safe to re-run)
./deploy-connectors.sh
```

## Flink Job Management

```bash
# Monitor Flink job logs
docker logs flink-jobmanager

# Deploy/restart the averaging job
./deploy-flink-job.sh
```

The Flink table definitions (`iot_devices_source` and `iot_devices_avg`) and the INSERT job are both submitted by `deploy-flink-job.sh`. The script uses the `flink-sql-client` container to run the SQL non-interactively.

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
