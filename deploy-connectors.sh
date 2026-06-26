#!/bin/bash
# Deploy all Kafka Connect connectors (source + sinks)

set -e

CONNECT_HOST="${CONNECT_HOST:-localhost:8083}"

# ---------------------------------------------------------------------------
wait_for_connect() {
    echo "Waiting for Kafka Connect at http://$CONNECT_HOST ..."
    for i in $(seq 1 60); do
        if curl -sf "http://$CONNECT_HOST/" -o /dev/null; then
            echo "Kafka Connect is ready."
            return 0
        fi
        echo "  attempt $i/60 ..."
        sleep 5
    done
    echo "ERROR: Kafka Connect did not become ready in time." >&2
    exit 1
}

SCHEMA_REGISTRY_HOST="${SCHEMA_REGISTRY_HOST:-localhost:8081}"

wait_for_schema_registry() {
    echo "Waiting for Schema Registry at http://$SCHEMA_REGISTRY_HOST ..."
    for i in $(seq 1 30); do
        if curl -sf "http://$SCHEMA_REGISTRY_HOST/subjects" -o /dev/null; then
            echo "Schema Registry is ready."
            return 0
        fi
        echo "  attempt $i/30 ..."
        sleep 5
    done
    echo "ERROR: Schema Registry did not become ready in time." >&2
    exit 1
}

# Wait until DB2 accepts JDBC connections AND the IOT_DEVICES table exists.
# DB2 accepts connections before init-db.sh finishes creating the table,
# so we must check the table explicitly or the source connector gets SQLCODE=-204.
wait_for_db2() {
    echo "Waiting for DB2 + IOT_DEVICES table (can take 3-5 min on first boot)..."
    local start
    start=$(date +%s)
    local attempt=0
    local max=72   # 72 x 10s = 12 minutes max

    while [ $attempt -lt $max ]; do
        attempt=$((attempt + 1))
        local elapsed=$(( $(date +%s) - start ))

        # Fast-fail: container must be running
        local cstate
        cstate=$(docker inspect db2-luw --format='{{.State.Status}}' 2>/dev/null || echo "missing")
        if [ "$cstate" != "running" ]; then
            echo ""
            echo "ERROR: db2-luw is '$cstate' — check logs: docker logs db2-luw" >&2
            exit 1
        fi

        # Check both: DB2 accepts connections AND the table exists
        if docker exec db2-luw su - db2inst1 \
               -c 'db2 connect to testdb > /dev/null 2>&1 &&
                   db2 "SELECT COUNT(*) FROM DB2INST1.IOT_DEVICES" > /dev/null 2>&1 &&
                   db2 connect reset > /dev/null 2>&1' \
               2>/dev/null; then
            echo "DB2 ready and IOT_DEVICES table exists (${elapsed}s)."
            return 0
        fi

        printf "  [%3ds] attempt %d/%d — waiting for DB2 and IOT_DEVICES table...\n" \
               "$elapsed" "$attempt" "$max"
        sleep 10
    done
    echo "ERROR: DB2/IOT_DEVICES did not become ready within $(( max * 10 ))s." >&2
    exit 1
}

deploy() {
    local name="$1"
    local file="$2"
    echo ""
    echo "--- Deploying $name ---"

    # Remove existing instance if present
    if curl -sf "http://$CONNECT_HOST/connectors/$name" -o /dev/null; then
        echo "  removing existing connector..."
        curl -sX DELETE "http://$CONNECT_HOST/connectors/$name"
        sleep 2
    fi

    response=$(curl -sf -X POST \
        -H "Content-Type: application/json" \
        --data "@$file" \
        "http://$CONNECT_HOST/connectors")

    echo "  deployed: $(echo "$response" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["name"])' 2>/dev/null || echo 'ok')"
}

connector_state() {
    curl -sf "http://$CONNECT_HOST/connectors/$1/status" \
        | python3 -c 'import sys,json; print(json.load(sys.stdin)["connector"]["state"])' 2>/dev/null || echo "UNKNOWN"
}

restart_if_failed() {
    local name="$1"
    local tries=5
    for i in $(seq 1 $tries); do
        local st
        st=$(connector_state "$name")
        if [ "$st" = "RUNNING" ]; then
            echo "  $name: RUNNING"
            return 0
        fi
        echo "  $name: $st — restarting (attempt $i/$tries)..."
        curl -sX POST "http://$CONNECT_HOST/connectors/$name/restart" -o /dev/null
        sleep 6
    done
    echo "  WARNING: $name still not RUNNING after $tries restart attempts"
}

# ---------------------------------------------------------------------------
wait_for_connect
wait_for_schema_registry
wait_for_db2

deploy "db2-source-connector" "connectors/db2-source-connector.json"
deploy "postgres-iot-devices-sink" "connectors/postgres-sink-connector.json"
deploy "redis-iot-devices-sink"    "connectors/redis-sink-connector.json"

echo ""
echo "Waiting for connectors to start..."
sleep 8

echo ""
echo "Checking and recovering any FAILED connectors..."
restart_if_failed "db2-source-connector"
restart_if_failed "postgres-iot-devices-sink"
restart_if_failed "redis-iot-devices-sink"

echo ""
echo "=== Final status ==="
curl -sf "http://$CONNECT_HOST/connectors?expand=status" | \
    python3 -c '
import sys, json
data = json.load(sys.stdin)
for name, info in data.items():
    state = info.get("status", {}).get("connector", {}).get("state", "?")
    print(f"  {name:40s}  {state}")
' 2>/dev/null
echo ""
