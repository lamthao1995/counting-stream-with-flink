package load

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/airwallex/heartbeat/pkg/kafkax"
	"github.com/airwallex/heartbeat/pkg/model"
)

// We reuse the ingestion server directly here by running an in-process
// HTTP server with a FakeProducer. That way the stress test exercises
// the exact production code path.
func newIngestionTestServer(t *testing.T) (*httptest.Server, *kafkax.FakeProducer, *atomic.Uint64) {
	t.Helper()
	fake := &kafkax.FakeProducer{}
	var rps atomic.Uint64
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		rps.Add(1)
		var h model.Heartbeat
		if err := json.NewDecoder(r.Body).Decode(&h); err != nil {
			w.WriteHeader(400)
			return
		}
		if err := h.Validate(time.Now(), 2*time.Minute); err != nil {
			w.WriteHeader(422)
			return
		}
		_ = fake.Publish(r.Context(), []model.Heartbeat{h})
		w.WriteHeader(202)
	})
	mux.HandleFunc("POST /api/v1/heartbeats:batch", func(w http.ResponseWriter, r *http.Request) {
		rps.Add(1)
		var req model.BatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(400)
			return
		}
		for _, h := range req.Heartbeats {
			if err := h.Validate(time.Now(), 2*time.Minute); err != nil {
				w.WriteHeader(422)
				return
			}
		}
		_ = fake.Publish(r.Context(), req.Heartbeats)
		w.WriteHeader(202)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, fake, &rps
}

func TestRun_SingleRequest(t *testing.T) {
	srv, fake, _ := newIngestionTestServer(t)
	res, err := Run(context.Background(), Config{
		Target:     srv.URL,
		RPS:        500,
		Duration:   400 * time.Millisecond,
		Workers:    8,
		BatchSize:  1,
		DevicePool: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Sent == 0 || res.Accepted == 0 {
		t.Fatalf("no traffic: %+v", res)
	}
	if res.Rejected != 0 {
		t.Fatalf("unexpected rejects: %+v", res)
	}
	// A few requests may race with ctx cancellation at shutdown; we
	// tolerate a small ratio instead of demanding zero.
	if res.NetworkErrs > res.Accepted/20 {
		t.Fatalf("too many net errs: %d (accepted=%d)", res.NetworkErrs, res.Accepted)
	}
	if n := len(fake.Snapshot()); uint64(n) != res.Accepted {
		t.Fatalf("producer got %d, result says accepted=%d", n, res.Accepted)
	}
	slog.Info("stress", "avg_ms", res.Avg().Milliseconds(), "p99_ms", res.Percentile(0.99).Milliseconds())
}

func TestRun_Batched(t *testing.T) {
	srv, fake, _ := newIngestionTestServer(t)
	res, err := Run(context.Background(), Config{
		Target:     srv.URL,
		RPS:        200,
		Duration:   400 * time.Millisecond,
		Workers:    4,
		BatchSize:  10,
		DevicePool: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Sent < res.Accepted {
		t.Fatalf("sent=%d < accepted=%d", res.Sent, res.Accepted)
	}
	if got := uint64(len(fake.Snapshot())); got != res.Accepted {
		t.Fatalf("snapshot=%d accepted=%d", got, res.Accepted)
	}
}

func TestRun_TargetRequired(t *testing.T) {
	if _, err := Run(context.Background(), Config{}); err == nil {
		t.Fatalf("want error")
	}
}

func TestPercentileAndAvg(t *testing.T) {
	r := Result{Latencies: []time.Duration{
		10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond,
		40 * time.Millisecond, 100 * time.Millisecond,
	}}
	if r.Avg() != 40*time.Millisecond {
		t.Fatalf("avg=%v", r.Avg())
	}
	if r.Percentile(0.99) != 100*time.Millisecond {
		t.Fatalf("p99=%v", r.Percentile(0.99))
	}
}

func TestRun_RespectsContextCancel(t *testing.T) {
	srv, _, _ := newIngestionTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(80 * time.Millisecond); cancel() }()
	start := time.Now()
	_, err := Run(ctx, Config{
		Target: srv.URL, RPS: 5000, Duration: 5 * time.Second,
		Workers: 8, BatchSize: 1, DevicePool: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("cancel not respected, ran for %s", time.Since(start))
	}
}

// Ensure the generator drains the response body so keep-alive connections
// are reused (otherwise we'd saturate ephemeral ports under load).
func TestRun_ReusesConnections(t *testing.T) {
	var hits atomic.Uint64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(202)
	}))
	defer srv.Close()
	_, err := Run(context.Background(), Config{
		Target: srv.URL, RPS: 400, Duration: 300 * time.Millisecond,
		Workers: 4, BatchSize: 1, DevicePool: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if hits.Load() == 0 {
		t.Fatalf("no hits")
	}
}
