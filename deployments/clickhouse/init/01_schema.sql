-- Pre-aggregated rows written by the Flink job.
--
-- The sort key (window_start, region, status) matches the operator's
-- typical filter AND acts as the dedup key for ReplacingMergeTree: if
-- Flink restarts and re-emits the same window (the JDBC sink is
-- at-least-once even though the pipeline checkpoints exactly-once), the
-- retry collapses into one row on background merge. `ingested_at` is
-- the version column so the most recent insert wins, and `FINAL` in
-- read queries enforces the collapse before background merges catch up.
CREATE TABLE IF NOT EXISTS heartbeat_aggregates
(
    window_start   DateTime64(3),
    window_end     DateTime64(3),
    region         LowCardinality(String),
    status         LowCardinality(String),
    count          UInt64,
    distinct_count UInt64,
    avg_latency    Float64,
    p50_latency    Float64,
    p95_latency    Float64,
    p99_latency    Float64,
    ingested_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (window_start, region, status)
TTL toDateTime(window_start) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Raw heartbeats for exact distinct/ad-hoc queries. Caller-driven
-- retention is short (7d) - long-term storage belongs in S3/parquet.
CREATE TABLE IF NOT EXISTS heartbeats
(
    event_time  DateTime64(3),
    device_id   String,
    region      LowCardinality(String),
    status      LowCardinality(String),
    latency_ms  Float64,
    cpu_usage   Float32,
    mem_usage   Float32
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (event_time, region, device_id)
TTL toDateTime(event_time) + INTERVAL 7 DAY
SETTINGS index_granularity = 8192;
