#!/bin/bash
# Gracefully stop all containers, preserving data volumes.

set -e

if ! docker info > /dev/null 2>&1; then
  echo "ERROR: Docker is not running." >&2
  exit 1
fi

echo "Stopping demo stack and removing volumes..."
docker compose down -v --remove-orphans
echo "Done. Restart with: ./start.sh"
