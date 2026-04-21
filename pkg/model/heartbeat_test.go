package model

import (
	"strings"
	"testing"
	"time"
)

func TestStatus_IsValid(t *testing.T) {
	cases := map[Status]bool{
		StatusOK:           true,
		StatusWarn:         true,
		StatusError:        true,
		Status(""):         false,
		Status("critical"): false,
	}
	for s, want := range cases {
		if got := s.IsValid(); got != want {
			t.Errorf("Status(%q).IsValid() = %v, want %v", s, got, want)
		}
	}
}

func TestHeartbeat_Validate(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	skew := time.Minute

	base := func() Heartbeat {
		return Heartbeat{
			DeviceID:    "dev-1",
			Region:      "us-east",
			Status:      StatusOK,
			TimestampMs: now.Add(-10 * time.Second).UnixMilli(),
			LatencyMs:   42.0,
		}
	}

	tests := []struct {
		name    string
		mutate  func(*Heartbeat)
		wantErr string
	}{
		{"valid", func(*Heartbeat) {}, ""},
		{"empty device id", func(h *Heartbeat) { h.DeviceID = "  " }, "device_id"},
		{"oversized device id", func(h *Heartbeat) { h.DeviceID = strings.Repeat("x", 129) }, "device_id"},
		{"missing region", func(h *Heartbeat) { h.Region = "" }, "region"},
		{"bad status", func(h *Heartbeat) { h.Status = "on-fire" }, "status"},
		{"zero timestamp", func(h *Heartbeat) { h.TimestampMs = 0 }, "timestamp"},
		{"future timestamp", func(h *Heartbeat) {
			h.TimestampMs = now.Add(2 * time.Minute).UnixMilli()
		}, "future"},
		{"negative latency", func(h *Heartbeat) { h.LatencyMs = -1 }, "latency"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := base()
			tc.mutate(&h)
			err := h.Validate(now, skew)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("want no error, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want err containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}
