#!/usr/bin/env bash
# Fast reset: tear the stack down and bring it back up with a rebuild.
# Volumes (Kafka state, ClickHouse data, Flink checkpoints) are kept so the
# dataset survives; use `docker compose down -v` manually to wipe them.
set -euo pipefail

cd "$(dirname "$0")"

echo "[1/2] docker compose down"
docker compose --env-file .env -f deployments/docker-compose.yml down

echo "[2/2] docker compose up -d --build"
exec ./run-all.sh
