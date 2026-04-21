package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/airwallex/heartbeat/pkg/kafkax"
	"github.com/airwallex/heartbeat/pkg/model"
)

func testServer(t *testing.T, p kafkax.Producer) *Server {
	t.Helper()
	return New(Options{
		Producer: p,
		Now:      func() time.Time { return time.Unix(1_700_000_000, 0) },
	})
}

func goodHeartbeatJSON() string {
	return `{"device_id":"dev-1","region":"us-east","status":"ok",
		"timestamp_ms":1699999990000,"latency_ms":42.5}`
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestHandleSingle_Success(t *testing.T) {
	f := &kafkax.FakeProducer{}
	rr := do(t, testServer(t, f).Handler(), "POST", "/api/v1/heartbeat", goodHeartbeatJSON())
	if rr.Code != http.StatusAccepted {
		t.Fatalf("code=%d body=%s", rr.Code, rr.Body.String())
	}
	if got := f.Snapshot(); len(got) != 1 || got[0].DeviceID != "dev-1" {
		t.Fatalf("unexpected publish: %+v", got)
	}
}

func TestHandleSingle_BadJSON(t *testing.T) {
	f := &kafkax.FakeProducer{}
	rr := do(t, testServer(t, f).Handler(), "POST", "/api/v1/heartbeat", `{not json`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
	if len(f.Snapshot()) != 0 {
		t.Fatalf("want nothing published")
	}
}

func TestHandleSingle_ValidationFails(t *testing.T) {
	f := &kafkax.FakeProducer{}
	bad := `{"device_id":"","region":"us","status":"ok","timestamp_ms":1699999990000}`
	rr := do(t, testServer(t, f).Handler(), "POST", "/api/v1/heartbeat", bad)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d body=%s", rr.Code, rr.Body.String())
	}
}

func TestHandleSingle_PublishErrorReturns503(t *testing.T) {
	f := &kafkax.FakeProducer{Err: errors.New("broker down")}
	rr := do(t, testServer(t, f).Handler(), "POST", "/api/v1/heartbeat", goodHeartbeatJSON())
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", rr.Code)
	}
}

func TestHandleBatch_Success(t *testing.T) {
	f := &kafkax.FakeProducer{}
	body := `{"heartbeats":[` + goodHeartbeatJSON() + `,` + strings.Replace(goodHeartbeatJSON(), "dev-1", "dev-2", 1) + `]}`
	rr := do(t, testServer(t, f).Handler(), "POST", "/api/v1/heartbeats:batch", body)
	if rr.Code != http.StatusAccepted {
		t.Fatalf("code=%d body=%s", rr.Code, rr.Body.String())
	}
	if got := f.Snapshot(); len(got) != 2 {
		t.Fatalf("want 2 published, got %d", len(got))
	}
}

func TestHandleBatch_EmptyRejected(t *testing.T) {
	f := &kafkax.FakeProducer{}
	rr := do(t, testServer(t, f).Handler(), "POST", "/api/v1/heartbeats:batch", `{"heartbeats":[]}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rr.Code)
	}
}

func TestHandleBatch_TooLarge(t *testing.T) {
	f := &kafkax.FakeProducer{}
	s := New(Options{
		Producer: f,
		Now:      func() time.Time { return time.Unix(1_700_000_000, 0) },
		MaxBatch: 2,
	})
	body := `{"heartbeats":[` + goodHeartbeatJSON() + `,` + goodHeartbeatJSON() + `,` + goodHeartbeatJSON() + `]}`
	rr := do(t, s.Handler(), "POST", "/api/v1/heartbeats:batch", body)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413, got %d", rr.Code)
	}
}

func TestHandleBatch_AllOrNothingValidation(t *testing.T) {
	f := &kafkax.FakeProducer{}
	// second event has empty device id -> entire batch rejected
	body := `{"heartbeats":[` + goodHeartbeatJSON() + `,` +
		`{"device_id":"","region":"us","status":"ok","timestamp_ms":1699999990000,"latency_ms":1}]}`
	rr := do(t, testServer(t, f).Handler(), "POST", "/api/v1/heartbeats:batch", body)
	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("want 422, got %d", rr.Code)
	}
	if n := len(f.Snapshot()); n != 0 {
		t.Fatalf("want 0 published, got %d", n)
	}
}

func TestHandleHealthAndMetrics(t *testing.T) {
	f := &kafkax.FakeProducer{}
	h := testServer(t, f).Handler()
	rr := do(t, h, "GET", "/healthz", "")
	if rr.Code != 200 {
		t.Fatalf("health=%d", rr.Code)
	}
	// send one good + one bad so counters are non-trivial
	do(t, h, "POST", "/api/v1/heartbeat", goodHeartbeatJSON())
	do(t, h, "POST", "/api/v1/heartbeat", `{bad`)

	rr = do(t, h, "GET", "/metrics", "")
	var m map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &m)
	if m["accepted_total"].(float64) != 1 || m["rejected_total"].(float64) != 1 {
		t.Fatalf("metrics wrong: %+v", m)
	}
	if m["publish_failures_total"].(float64) != 0 {
		t.Fatalf("unexpected failures counter: %+v", m)
	}
}

// Publish failures (broker down etc.) must be reflected in the metrics
// endpoint separately from rejected so operators can distinguish client
// errors from broker problems.
func TestHandleMetrics_CountsPublishFailures(t *testing.T) {
	f := &kafkax.FakeProducer{Err: errors.New("broker down")}
	h := testServer(t, f).Handler()

	do(t, h, "POST", "/api/v1/heartbeat", goodHeartbeatJSON())
	batch := `{"heartbeats":[` + goodHeartbeatJSON() + `,` +
		strings.Replace(goodHeartbeatJSON(), "dev-1", "dev-2", 1) + `]}`
	do(t, h, "POST", "/api/v1/heartbeats:batch", batch)

	rr := do(t, h, "GET", "/metrics", "")
	var m map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &m)
	// 1 single + 2 batch events failed to publish; none were rejected by
	// validation; none were accepted.
	if m["publish_failures_total"].(float64) != 3 {
		t.Fatalf("want 3 publish failures, got %+v", m)
	}
	if m["accepted_total"].(float64) != 0 || m["rejected_total"].(float64) != 0 {
		t.Fatalf("unexpected counters: %+v", m)
	}
}

// concurrent fuzz-ish test: many writers slam the handler, make sure no
// data races and all accepted events reach the producer.
func TestHandleSingle_Concurrency(t *testing.T) {
	f := &kafkax.FakeProducer{}
	h := testServer(t, f).Handler()
	const N = 200
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"device_id":"dev-%d","region":"us-east","status":"ok","timestamp_ms":1699999990000,"latency_ms":%d}`, i, i)
			rr := do(t, h, "POST", "/api/v1/heartbeat", body)
			if rr.Code != http.StatusAccepted {
				t.Errorf("req %d code=%d", i, rr.Code)
			}
		}(i)
	}
	wg.Wait()
	if got := len(f.Snapshot()); got != N {
		t.Fatalf("want %d published, got %d", N, got)
	}
}

// confirm we honour request cancellation by passing context to Publish.
func TestHandleSingle_ContextCancelPropagated(t *testing.T) {
	f := &ctxSpyProducer{}
	h := testServer(t, f).Handler()
	req := httptest.NewRequest("POST", "/api/v1/heartbeat", strings.NewReader(goodHeartbeatJSON()))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req = req.WithContext(ctx)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if f.seenCancelled == false {
		t.Fatalf("producer never saw a cancelled context")
	}
}

type ctxSpyProducer struct{ seenCancelled bool }

func (c *ctxSpyProducer) Publish(ctx context.Context, _ []model.Heartbeat) error {
	if ctx.Err() != nil {
		c.seenCancelled = true
	}
	return nil
}
func (c *ctxSpyProducer) Close() error { return nil }

// belt-and-braces: ensures readLimited rejects oversized bodies.
func TestMaxBodyBytes(t *testing.T) {
	f := &kafkax.FakeProducer{}
	s := New(Options{
		Producer:     f,
		Now:          func() time.Time { return time.Unix(1_700_000_000, 0) },
		MaxBodyBytes: 50,
	})
	big := bytes.Repeat([]byte("x"), 200)
	req := httptest.NewRequest("POST", "/api/v1/heartbeat", io.NopCloser(bytes.NewReader(big)))
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413, got %d", rr.Code)
	}
}
