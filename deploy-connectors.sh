#!/bin/bash
# Deploy all Kafka Connect connectors (source + 2 sinks)

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

# Wait until DB2's TCPIP listener is fully accepting JDBC connections.
wait_for_db2() {
    echo "Waiting for DB2 to accept connections (can take 3-5 min on first boot)..."
    local start
    start=$(date +%s)
    local attempt=0
    local max=60   # 60 x 10s = 10 minutes max

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

        # Try a JDBC-level connect
        if docker exec db2-luw su - db2inst1 \
               -c 'db2 connect to testdb > /dev/null 2>&1 && db2 connect reset > /dev/null 2>&1' \
               2>/dev/null; then
            echo "DB2 is ready (${elapsed}s)."
            return 0
        fi

        printf "  [%3ds] attempt %d/%d — db2inst1 not yet accepting connections...\n" \
               "$elapsed" "$attempt" "$max"
        sleep 10
    done
    echo "ERROR: DB2 did not become ready within $(( max * 10 ))s." >&2
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
wait_for_db2

deploy "db2-source-connector"    "connectors/db2-source-connector.json"
deploy "postgres-sink-connector" "connectors/postgres-sink-connector.json"
deploy "redis-sink-connector"    "connectors/redis-sink-connector.json"

echo ""
echo "Waiting for connectors to start..."
sleep 8

echo ""
echo "Checking and recovering any FAILED connectors..."
restart_if_failed "db2-source-connector"
restart_if_failed "postgres-sink-connector"
restart_if_failed "redis-sink-connector"

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
