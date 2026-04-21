package model

// WindowAggregate is what the Flink job writes to ClickHouse per
// (window_start, region, status) key. The query service reads rows of this
// shape to serve the operator dashboard.
//
// We store t-digest / HLL state on the Flink side but flatten to the final
// numeric quantiles/distinct-count at write time to keep the read path
// cheap (no re-merging required for simple dashboards).
type WindowAggregate struct {
	WindowStartMs int64   `json:"window_start_ms" ch:"window_start"`
	WindowEndMs   int64   `json:"window_end_ms"   ch:"window_end"`
	Region        string  `json:"region"          ch:"region"`
	Status        string  `json:"status"          ch:"status"`
	Count         uint64  `json:"count"           ch:"count"`
	DistinctCount uint64  `json:"distinct_count"  ch:"distinct_count"`
	AvgLatencyMs  float64 `json:"avg_latency_ms"  ch:"avg_latency"`
	P50LatencyMs  float64 `json:"p50_latency_ms"  ch:"p50_latency"`
	P95LatencyMs  float64 `json:"p95_latency_ms"  ch:"p95_latency"`
	P99LatencyMs  float64 `json:"p99_latency_ms"  ch:"p99_latency"`
}
