# IBM Db2 → Confluent Platform - Live Pipeline Demo

This demo shows how to build a **live data pipeline** from IBM Db2 to Confluent Platform, with a **1:3 fanout** to three different sinks: PostgreSQL, Redis, and a Python frontend. The pipeline is fully containerized and runs locally using Docker Compose.

## Architecture

```
IBM Db2/LUW  (data generator: 10 rows, 1 op/sec)
    │
    │  JDBC Source Connector (poll every 1s)
    ▼
┌───────────────────────────────────────┐
│ Confluent Platform                    │
│   Kafka topic: db2-EMPLOYEES          │
│   Schema Registry (Avro)              │
└───────────────────────────────────────┘
    │               │               │
    ▼               ▼               ▼
PostgreSQL       Redis           Python Frontend
(JDBC Sink)   (Redis Sink)    (Kafka consumer +
                               WebSocket push)
```

**1:3 fanout** from a single Kafka topic `db2-EMPLOYEES`.

A **data generator** container continuously inserts and updates up to 10 rows in Db2 at one operation per second. Changes propagate through Confluent and appear live in all three sinks simultaneously.

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

The first run pulls all images and builds the custom containers - allow **5–10 minutes**. Subsequent restarts are fast (~30 s) because data volumes are preserved.

`start.sh` waits for Schema Registry, Kafka Connect, Control Center, and pgAdmin to be healthy, then prints the service URLs.

### 2. Deploy connectors

In a second terminal, once `start.sh` has printed the service URLs:

```bash
./deploy-connectors.sh
```

This waits for DB2 to accept JDBC connections, then deploys all three connectors (source + two sinks) and verifies they reach `RUNNING` state. The Python frontend is a direct Kafka consumer and needs no connector.

### 3. Watch the data flow

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

## Stopping and Starting

```bash
# Stop - preserves nothing (clean slate on next start)
./stop.sh

# Start - always does a full reset, DB2 re-initialises from scratch (3-5 min)
./start.sh
```

## Service Details

### IBM Db2 (`localhost:50000`)

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
db2 "SELECT * FROM DB2INST1.EMPLOYEES"
db2 "SELECT COUNT(*) FROM DB2INST1.EMPLOYEES"
```

Watch rows being updated live (refreshes every second):
```bash
./watch-db2.sh
```

### PostgreSQL (`localhost:5432`)

```
Database: postgres
User    : postgres
Password: postgres
```

```bash
docker exec -it postgres psql -U postgres -d postgres -c "SELECT * FROM employees;"
```

pgAdmin is pre-configured with a **Demo - PostgreSQL** server. Open http://localhost:5050 and expand the server tree - no manual setup required.

### Redis (`localhost:6379`)

Employee records are stored as RedisJSON under keys `employee:<id>`.

```bash
# List all employee keys
docker exec -it redis redis-cli KEYS "employee:*"

# Read one record
docker exec -it redis redis-cli JSON.GET employee:1

# Total key count
docker exec -it redis redis-cli DBSIZE
```

Browse all keys visually at http://localhost:8087 (Redis Commander).

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

## Kafka

```bash
# List topics
docker exec broker kafka-topics --bootstrap-server localhost:9092 --list

# Consume decoded records (Avro → JSON via Schema Registry)
docker exec schema-registry kafka-avro-console-consumer \
  --bootstrap-server broker:29092 \
  --topic db2-EMPLOYEES \
  --from-beginning

# Check consumer group lag
docker exec broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group connect-postgres-sink-connector \
  --describe
```

## Troubleshooting

**DB2 data generator shows "not ready yet" on startup** - normal. DB2 takes 15–30 s to resume from a preserved volume, and 3–5 min on a full reset. The generator loops patiently until the JDBC port is open.

**Source connector in FAILED state** - run `./deploy-connectors.sh` again; the script automatically waits for DB2 and retries failed connectors.

**pgAdmin "no password supplied" error** - `start.sh` seeds the password automatically. If you skip `start.sh` and start containers manually, run:
```bash
./start.sh   # re-seeds the pgAdmin password as part of startup
```

**Port conflict** - check which process owns the port:
```bash
lsof -i :50000   # DB2
lsof -i :9021    # Control Center
```

## Resources

- [Confluent JDBC Connector docs](https://docs.confluent.io/kafka-connectors/jdbc/current/index.html)
- [Redis Kafka Connector docs](https://redis.io/docs/latest/integrate/kafka/)
- [IBM Db2 Community Edition](https://www.ibm.com/docs/en/db2/11.5)
- [Confluent Control Center](http://localhost:9021)
