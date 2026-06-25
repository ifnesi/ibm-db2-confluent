#!/bin/bash
# Start the full demo stack.
#
# Normal restart (keeps DB2/Postgres/Redis data — DB2 ready in ~30s):
#   ./start.sh
#
# Full reset (wipes all volumes — DB2 re-initialises from scratch, takes 3-5 min):

set -e

if ! docker info > /dev/null 2>&1; then
  echo "ERROR: Docker is not running. Please start Docker Desktop." >&2
  exit 1
fi

echo "Stopping any previous stack and removing volumes..."
docker compose down -v --remove-orphans 2>/dev/null || true
echo ""

echo "Building custom images (no cache to ensure latest code is used)..."
docker compose build --no-cache db2-data-generator frontend

echo "Starting all services..."
docker compose up -d

# ---------------------------------------------------------------------------
wait_for() {
  local label="$1"
  local url="$2"
  local max="${3:-60}"
  local attempt=0
  local start
  start=$(date +%s)

  printf "  %-22s " "$label"
  while [ $attempt -lt $max ]; do
    if curl -sf -o /dev/null "$url"; then
      local elapsed=$(( $(date +%s) - start ))
      echo "ready (${elapsed}s)"
      return 0
    fi
    attempt=$((attempt + 1))
    printf "."
    sleep 5
  done
  echo " TIMED OUT after $((max * 5))s"
  return 1
}

# wait_for_cmd: poll a shell command (passed as a single string) instead of an HTTP URL
wait_for_cmd() {
  local label="$1"
  local cmd="$2"
  local max="${3:-60}"
  local attempt=0
  local start
  start=$(date +%s)

  printf "  %-22s " "$label"
  while [ $attempt -lt $max ]; do
    if bash -c "$cmd" > /dev/null 2>&1; then
      local elapsed=$(( $(date +%s) - start ))
      echo "ready (${elapsed}s)"
      return 0
    fi
    attempt=$((attempt + 1))
    printf "."
    sleep 5
  done
  echo " TIMED OUT after $((max * 5))s"
  return 1
}

echo ""
echo "Waiting for core services (allow 5~10 minutes due to DB2 initialisation)..."

wait_for "Schema Registry"   "http://localhost:8081/"        60
wait_for "Kafka Connect"     "http://localhost:8083/"        60
wait_for "Control Center"    "http://localhost:9021/login"   90
wait_for "pgAdmin"           "http://localhost:5050/login"   30
wait_for "Flink"             "http://localhost:9081/"        30
wait_for "Flask Frontend"    "http://localhost:5001/"        30
wait_for_cmd "Redis"         "docker exec redis redis-cli ping"                               30
wait_for "Redis Commander"   "http://localhost:8087/"                                         30
wait_for_cmd "DB2"           "docker exec db2-luw su - db2inst1 -c 'db2 connect to testdb'"  120

# Seed pgAdmin server password (servers.json cannot carry passwords)
echo ""
echo "Seeding pgAdmin server password..."
_pgadmin_email=$(grep PGADMIN_DEFAULT_EMAIL .env 2>/dev/null | tail -1 | cut -d= -f2-)
_pgadmin_pass=$(grep PGADMIN_DEFAULT_PASSWORD .env 2>/dev/null | tail -1 | cut -d= -f2-)
_pg_pass=$(grep POSTGRES_PASSWORD .env 2>/dev/null | tail -1 | cut -d= -f2-)
_csrf=$(curl -sc /tmp/pgadmin_cookie_start.txt -sX POST "http://localhost:5050/login" \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"${_pgadmin_email}\",\"password\":\"${_pgadmin_pass}\"}" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["response"]["csrf_token"])' 2>/dev/null)
if [ -n "$_csrf" ]; then
  curl -sb /tmp/pgadmin_cookie_start.txt -sX PUT "http://localhost:5050/browser/server/obj/1/1" \
    -H "Content-Type: application/json" \
    -H "X-pgA-CSRFToken: $_csrf" \
    -d "{\"password\":\"${_pg_pass}\",\"save_password\":true}" > /dev/null
  echo "  pgAdmin password seeded."
else
  echo "  WARNING: could not seed pgAdmin password — log in manually."
fi

echo ""
echo "Services:"
echo "  Control Center     http://localhost:9021"
echo "  Flink Job Manager  http://localhost:9081"
echo "  Live Dashboard     http://localhost:5001"
echo "  pgAdmin            http://localhost:5050   (${_pgadmin_email} / ${_pgadmin_pass})"
echo "  Redis Commander    http://localhost:8087"
echo "  Kafka Connect      http://localhost:8083"
echo "  Schema Registry    http://localhost:8081"
echo ""
echo "Next steps:"
echo "  1. Deploy connectors (waits for DB2 automatically):"
echo "     ./deploy-connectors.sh"
echo ""
echo "  2. Deploy Flink averaging job:"
echo "     ./deploy-flink-job.sh"
echo ""
echo "Monitor: docker compose logs -f"
