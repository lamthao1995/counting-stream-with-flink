// Package model defines wire types shared between ingestion, query service
// and the Flink job. Any change here is a breaking protocol change, so keep
// field names/tags stable and version when needed.
package model

import (
	"errors"
	"strings"
	"time"
)

// Status enumerates the canonical device health states. We keep it a plain
// string (not int) so downstream OLAP queries stay human-readable.
type Status string

const (
	StatusOK    Status = "ok"
	StatusWarn  Status = "warn"
	StatusError Status = "error"
)

// IsValid reports whether s is a known status.
func (s Status) IsValid() bool {
	switch s {
	case StatusOK, StatusWarn, StatusError:
		return true
	default:
		return false
	}
}

// Heartbeat is the raw event emitted by an IoT device.
//
// LatencyMs is the value we compute p95/p99/avg on; we keep CPUUsage /
// MemUsage as additional fleet-health signals the operator may want later.
type Heartbeat struct {
	DeviceID    string  `json:"device_id"`
	Region      string  `json:"region"`
	Status      Status  `json:"status"`
	TimestampMs int64   `json:"timestamp_ms"`
	LatencyMs   float64 `json:"latency_ms"`
	CPUUsage    float64 `json:"cpu_usage,omitempty"`
	MemUsage    float64 `json:"mem_usage,omitempty"`
}

// Validate enforces the minimum set of invariants we want to guarantee
// before publishing to Kafka. We deliberately keep validation cheap so it
// can run on the ingestion hot path.
func (h *Heartbeat) Validate(now time.Time, maxClockSkew time.Duration) error {
	if strings.TrimSpace(h.DeviceID) == "" {
		return errors.New("device_id is required")
	}
	if len(h.DeviceID) > 128 {
		return errors.New("device_id too long")
	}
	if strings.TrimSpace(h.Region) == "" {
		return errors.New("region is required")
	}
	if !h.Status.IsValid() {
		return errors.New("status must be one of ok|warn|error")
	}
	if h.TimestampMs <= 0 {
		return errors.New("timestamp_ms must be positive")
	}
	// Reject events from too far in the future; we accept old events up to
	// retention window since IoT devices may buffer while offline.
	ts := time.UnixMilli(h.TimestampMs)
	if ts.After(now.Add(maxClockSkew)) {
		return errors.New("timestamp_ms is in the future beyond allowed skew")
	}
	if h.LatencyMs < 0 {
		return errors.New("latency_ms must be non-negative")
	}
	return nil
}

// BatchRequest is the wire format for POST /api/v1/heartbeats:batch.
type BatchRequest struct {
	Heartbeats []Heartbeat `json:"heartbeats"`
}
