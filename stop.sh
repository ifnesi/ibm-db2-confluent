#!/bin/bash
# Gracefully stop all containers, preserving data volumes.

set -e

if ! docker info > /dev/null 2>&1; then
  echo "ERROR: Docker is not running." >&2
  exit 1
fi

echo "Stopping demo stack and removing volumes..."
docker compose down -v --remove-orphans

echo "Removing dangling (<none>) images..."
docker images -f "dangling=true" -q | xargs docker rmi 2>/dev/null || true

echo "Done!"
