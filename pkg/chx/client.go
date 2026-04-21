// Package chx wraps the ClickHouse driver with query helpers used by
// the query service. It is intentionally thin so tests can stub the
// QueryRunner interface without mocking the whole driver.
package chx

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/airwallex/heartbeat/pkg/model"
)

// QueryRunner is the narrow surface our handlers depend on. Production
// code uses a ClickHouse-backed impl; tests use a fake.
type QueryRunner interface {
	FetchAggregates(ctx context.Context, f Filter) ([]model.WindowAggregate, error)
	DistinctDevices(ctx context.Context, f Filter) (uint64, error)
	Ping(ctx context.Context) error
	Close() error
}

// Filter bundles query parameters for aggregate fetches. All fields are
// optional except the time range.
//
// Note: there is no per-device filter. The aggregates table is keyed by
// (region, status, window) so a device_id predicate is meaningless, and
// filtering the raw heartbeats by a single device_id before uniqExact()
// would only ever return 0 or 1. If you need per-device lookups, query
// the raw `heartbeats` table via ClickHouse Play or a dedicated
// endpoint (not shipped yet).
type Filter struct {
	FromMs int64
	ToMs   int64
	Region string
	Status string
	Limit  int
}

// Config for the real ClickHouse client.
type Config struct {
	Addrs    []string
	Database string
	Username string
	Password string
	DialTO   time.Duration
}

// Client is the ClickHouse-backed QueryRunner.
type Client struct {
	db *sql.DB
}

// New constructs a Client ready to serve queries.
func New(cfg Config) (*Client, error) {
	if cfg.DialTO == 0 {
		cfg.DialTO = 5 * time.Second
	}
	db := clickhouse.OpenDB(&clickhouse.Options{
		Addr:        cfg.Addrs,
		Auth:        clickhouse.Auth{Database: cfg.Database, Username: cfg.Username, Password: cfg.Password},
		DialTimeout: cfg.DialTO,
	})
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(20)
	db.SetConnMaxLifetime(time.Hour)
	return &Client{db: db}, nil
}

func (c *Client) Ping(ctx context.Context) error { return c.db.PingContext(ctx) }
func (c *Client) Close() error                   { return c.db.Close() }

// FetchAggregates returns the pre-aggregated rows written by the Flink
// job. The operator dashboard renders these directly.
func (c *Client) FetchAggregates(ctx context.Context, f Filter) ([]model.WindowAggregate, error) {
	if f.FromMs <= 0 || f.ToMs <= 0 || f.FromMs >= f.ToMs {
		return nil, fmt.Errorf("invalid time range [%d, %d)", f.FromMs, f.ToMs)
	}
	if f.Limit <= 0 {
		f.Limit = 1000
	}

	// We build the WHERE clause dynamically, but always with positional
	// placeholders - never string concatenation of user input.
	//
	// FINAL forces the ReplacingMergeTree merge at read time so the API
	// never returns duplicate rows from a Flink retry/recovery. It is
	// slower than a plain SELECT but the aggregates table is small
	// (one row per minute per region/status) so the overhead is
	// negligible in practice.
	q := `SELECT toUnixTimestamp64Milli(window_start), toUnixTimestamp64Milli(window_end),
			region, status, count, distinct_count,
			avg_latency, p50_latency, p95_latency, p99_latency
		  FROM heartbeat_aggregates FINAL
		  WHERE window_start >= fromUnixTimestamp64Milli(?)
		    AND window_start <  fromUnixTimestamp64Milli(?)`
	args := []any{f.FromMs, f.ToMs}
	if f.Region != "" {
		q += ` AND region = ?`
		args = append(args, f.Region)
	}
	if f.Status != "" {
		q += ` AND status = ?`
		args = append(args, f.Status)
	}
	q += ` ORDER BY window_start ASC LIMIT ?`
	args = append(args, f.Limit)

	rows, err := c.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []model.WindowAggregate
	for rows.Next() {
		var a model.WindowAggregate
		if err := rows.Scan(
			&a.WindowStartMs, &a.WindowEndMs,
			&a.Region, &a.Status, &a.Count, &a.DistinctCount,
			&a.AvgLatencyMs, &a.P50LatencyMs, &a.P95LatencyMs, &a.P99LatencyMs,
		); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// DistinctDevices runs a global distinct-device count over the raw
// heartbeats table. For dashboards over a long window, the caller should
// instead SUM(distinct_count) on the aggregates table - this method is
// kept for ad-hoc operator queries.
func (c *Client) DistinctDevices(ctx context.Context, f Filter) (uint64, error) {
	if f.FromMs <= 0 || f.ToMs <= 0 || f.FromMs >= f.ToMs {
		return 0, fmt.Errorf("invalid time range")
	}
	q := `SELECT uniqExact(device_id) FROM heartbeats
		  WHERE event_time >= fromUnixTimestamp64Milli(?)
		    AND event_time <  fromUnixTimestamp64Milli(?)`
	args := []any{f.FromMs, f.ToMs}
	if f.Region != "" {
		q += ` AND region = ?`
		args = append(args, f.Region)
	}
	if f.Status != "" {
		q += ` AND status = ?`
		args = append(args, f.Status)
	}
	var n uint64
	return n, c.db.QueryRowContext(ctx, q, args...).Scan(&n)
}
