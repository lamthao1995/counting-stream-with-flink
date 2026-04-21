package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/airwallex/heartbeat/pkg/chx"
	"github.com/airwallex/heartbeat/pkg/model"
)

func newTestServer(r chx.QueryRunner) *Server {
	return New(Options{
		Runner: r,
		Now:    func() time.Time { return time.Unix(1_700_000_000, 0) },
	})
}

func get(t *testing.T, h http.Handler, url string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("GET", url, nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestParseFilter(t *testing.T) {
	s := newTestServer(&chx.FakeRunner{})
	tests := []struct {
		name    string
		qs      string
		wantErr string
	}{
		{"missing range", "", "from and to"},
		{"bad from", "from=abc&to=200", "invalid from"},
		{"to <= from", "from=200&to=100", "to must be greater"},
		{"region ok", "from=0&to=1000&region=us", ""},
		{"bad status", "from=0&to=1000&status=panic", "status must be"},
		{"window too big", fmt.Sprintf("from=0&to=%d", 8*24*int64(time.Hour/time.Millisecond)), "exceeds max"},
		{"bad limit", "from=0&to=1000&limit=-5", "invalid limit"},
		{"device_id rejected", "from=0&to=1000&device_id=dev-1", "device_id filter is not supported"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "http://x/?"+tc.qs, nil)
			_, err := s.parseFilter(req)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !contains(err.Error(), tc.wantErr)) {
				t.Fatalf("want err containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func contains(s, sub string) bool { return len(sub) == 0 || indexOf(s, sub) >= 0 }
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func TestHandleMetrics_Success(t *testing.T) {
	r := &chx.FakeRunner{
		Aggregates: []model.WindowAggregate{
			{WindowStartMs: 100, Region: "us", Status: "ok", Count: 5, AvgLatencyMs: 10},
			{WindowStartMs: 200, Region: "eu", Status: "ok", Count: 7, AvgLatencyMs: 20},
		},
	}
	s := newTestServer(r)
	rr := get(t, s.Handler(), "/api/v1/metrics?from=0&to=1000&region=us")
	if rr.Code != 200 {
		t.Fatalf("code=%d body=%s", rr.Code, rr.Body.String())
	}
	var body struct {
		Count  int                     `json:"count"`
		Series []model.WindowAggregate `json:"series"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &body)
	if body.Count != 1 || body.Series[0].Region != "us" {
		t.Fatalf("unexpected: %+v", body)
	}
	if r.LastFilter.Region != "us" {
		t.Fatalf("runner did not receive region filter")
	}
}

func TestHandleMetrics_RunnerError(t *testing.T) {
	r := &chx.FakeRunner{Err: errors.New("ch down")}
	rr := get(t, newTestServer(r).Handler(), "/api/v1/metrics?from=0&to=1000")
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("want 500, got %d", rr.Code)
	}
}

func TestHandleDistinct(t *testing.T) {
	r := &chx.FakeRunner{Distinct: 42}
	rr := get(t, newTestServer(r).Handler(), "/api/v1/distinct?from=0&to=1000&region=us")
	if rr.Code != 200 {
		t.Fatalf("code=%d", rr.Code)
	}
	var body struct {
		N uint64 `json:"distinct_devices"`
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &body)
	if body.N != 42 {
		t.Fatalf("want 42, got %d", body.N)
	}
}

// device_id used to be parsed but silently ignored, which returned
// broader data than the caller asked for. We now reject it at the
// boundary - see issue/003-device-id-filter-ignored.md.
func TestHandleMetrics_DeviceIDRejected(t *testing.T) {
	r := &chx.FakeRunner{}
	rr := get(t, newTestServer(r).Handler(), "/api/v1/metrics?from=0&to=1000&device_id=dev-1")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleDashboard_Rollup(t *testing.T) {
	r := &chx.FakeRunner{
		Aggregates: []model.WindowAggregate{
			{WindowStartMs: 100, Count: 10, AvgLatencyMs: 10, P95LatencyMs: 50, P99LatencyMs: 100},
			{WindowStartMs: 200, Count: 90, AvgLatencyMs: 20, P95LatencyMs: 60, P99LatencyMs: 120},
		},
		Distinct: 7,
	}
	rr := get(t, newTestServer(r).Handler(), "/api/v1/dashboard?from=0&to=1000")
	if rr.Code != 200 {
		t.Fatalf("code=%d", rr.Code)
	}
	var v DashboardView
	_ = json.Unmarshal(rr.Body.Bytes(), &v)
	if v.TotalEvents != 100 || v.DistinctDevices != 7 {
		t.Fatalf("totals wrong: %+v", v)
	}
	// count-weighted avg = (10*10 + 20*90)/100 = 19
	if math.Abs(v.AvgLatencyMs-19.0) > 1e-9 {
		t.Fatalf("weighted avg = %v, want 19", v.AvgLatencyMs)
	}
	// p95 weighted = (50*10 + 60*90)/100 = 59
	if math.Abs(v.P95LatencyMs-59.0) > 1e-9 {
		t.Fatalf("weighted p95 = %v, want 59", v.P95LatencyMs)
	}
}

func TestHandleDashboard_EmptyNoNaN(t *testing.T) {
	rr := get(t, newTestServer(&chx.FakeRunner{}).Handler(), "/api/v1/dashboard?from=0&to=1000")
	if rr.Code != 200 {
		t.Fatalf("code=%d", rr.Code)
	}
	var v DashboardView
	_ = json.Unmarshal(rr.Body.Bytes(), &v)
	if math.IsNaN(v.AvgLatencyMs) || v.AvgLatencyMs != 0 {
		t.Fatalf("empty rollup should return zero, got %v", v.AvgLatencyMs)
	}
}

func TestHealth_PingError(t *testing.T) {
	rr := get(t, newTestServer(&chx.FakeRunner{Err: errors.New("no conn")}).Handler(), "/healthz")
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", rr.Code)
	}
}
