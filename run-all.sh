#!/usr/bin/env bash
# One-shot local bootstrap: brings the full stack up and submits the Flink job.
set -euo pipefail

cd "$(dirname "$0")"

[[ -f .env ]] || cp .env.example .env
echo "[1/4] .env ready (edit to customise)."

echo "[2/4] docker compose up -d --build"
docker compose --env-file .env -f deployments/docker-compose.yml up -d --build

echo "[3/4] waiting for Kafka + ClickHouse to become healthy..."
for i in {1..60}; do
  kafka=$(docker compose --env-file .env -f deployments/docker-compose.yml ps --format json kafka | grep -o '"Health":"healthy"' || true)
  ch=$(docker compose --env-file .env -f deployments/docker-compose.yml ps --format json clickhouse | grep -o '"Health":"healthy"' || true)
  if [[ -n "$kafka" && -n "$ch" ]]; then echo "    healthy after ${i}s"; break; fi
  sleep 1
done

echo "[4/4] submitting Flink job"
docker compose --env-file .env -f deployments/docker-compose.yml exec -T flink-jobmanager \
  flink run -d /opt/flink/usrlib/flink-aggregator.jar || {
    echo "!! flink run failed - the job-builder may still be compiling."
    echo "   try again in a few seconds: make submit-flink"
  }

cat <<EOF

  Services:
    ingestion      http://localhost:\${INGESTION_HOST_PORT:-8080}
    query          http://localhost:\${QUERY_HOST_PORT:-8081}
    Flink UI       http://localhost:8082
    ClickHouse     http://localhost:8123  (UI: /play)

  Try it:
    curl -s -X POST http://localhost:8080/api/v1/heartbeat \\
      -H 'Content-Type: application/json' \\
      -d '{"device_id":"dev-1","region":"us-east","status":"ok",
           "timestamp_ms":'\$(date +%s%3N)',"latency_ms":23.5}'

    make stress

  Stop: ./restart-all.sh   (keeps volumes)
        docker compose down -v   (wipe everything)
EOF
