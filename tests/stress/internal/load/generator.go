// Package load is a small, allocation-conscious load generator for the
// heartbeat ingestion API. Exposed as a library so both the `stress` CLI
// and the `TestStress_*` suite can drive it.
package load

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/airwallex/heartbeat/pkg/model"
)

// Config drives the load generator.
type Config struct {
	Target       string        // e.g. http://localhost:8080
	RPS          int           // aggregate requests/second across workers
	Duration     time.Duration // total wall clock to run
	Workers      int           // concurrency level
	BatchSize    int           // heartbeats per request (>=1)
	DevicePool   int           // number of unique device IDs to draw from
	Regions      []string
	HTTPClient   *http.Client
	RandomSource rand.Source
}

// Result is the final report.
type Result struct {
	Sent        uint64
	Accepted    uint64
	Rejected    uint64
	NetworkErrs uint64
	Latencies   []time.Duration // one per HTTP request
}

// Percentile returns the p-th percentile (0..1) of the recorded request
// latencies using the nearest-rank method. The slice is left sorted in
// place as a side effect.
func (r *Result) Percentile(p float64) time.Duration {
	n := len(r.Latencies)
	if n == 0 {
		return 0
	}
	sortDurations(r.Latencies)
	if p <= 0 {
		return r.Latencies[0]
	}
	if p >= 1 {
		return r.Latencies[n-1]
	}
	idx := int(math.Ceil(p*float64(n))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return r.Latencies[idx]
}

func (r *Result) Avg() time.Duration {
	if len(r.Latencies) == 0 {
		return 0
	}
	var sum time.Duration
	for _, l := range r.Latencies {
		sum += l
	}
	return sum / time.Duration(len(r.Latencies))
}

// Run generates load according to cfg until ctx is cancelled or the
// duration elapses. It never panics; all errors are counted and surfaced
// through Result.
func Run(ctx context.Context, cfg Config) (Result, error) {
	if cfg.Target == "" {
		return Result{}, fmt.Errorf("target required")
	}
	if cfg.RPS <= 0 {
		cfg.RPS = 1000
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 16
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1
	}
	if cfg.DevicePool <= 0 {
		cfg.DevicePool = 1000
	}
	if len(cfg.Regions) == 0 {
		cfg.Regions = []string{"us-east", "us-west", "eu-west", "ap-south"}
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        cfg.Workers * 4,
				MaxIdleConnsPerHost: cfg.Workers * 4,
				IdleConnTimeout:     30 * time.Second,
			},
		}
	}
	if cfg.RandomSource == nil {
		cfg.RandomSource = rand.NewSource(time.Now().UnixNano())
	}
	if cfg.Duration == 0 {
		cfg.Duration = 10 * time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()

	// token bucket: one tick per 1/RPS second. Buffered so workers can
	// momentarily get ahead; capacity matters less than the emission rate.
	interval := time.Second / time.Duration(cfg.RPS)
	if interval <= 0 {
		interval = time.Microsecond
	}
	tokens := make(chan struct{}, cfg.Workers*2)
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				close(tokens)
				return
			case <-t.C:
				select {
				case tokens <- struct{}{}:
				default: // drop -- caller is slower than RPS
				}
			}
		}
	}()

	var (
		sent, accepted, rejected, nerr atomic.Uint64
		latMu                          sync.Mutex
		lats                           = make([]time.Duration, 0, cfg.RPS*int(cfg.Duration/time.Second))
	)

	recordLat := func(d time.Duration) {
		latMu.Lock()
		lats = append(lats, d)
		latMu.Unlock()
	}

	var wg sync.WaitGroup
	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for range tokens {
				batch := buildBatch(rng, cfg)
				elapsed, accepted2, rejected2, err := postBatch(ctx, cfg, batch)
				sent.Add(uint64(len(batch)))
				if err != nil {
					nerr.Add(1)
					continue
				}
				accepted.Add(accepted2)
				rejected.Add(rejected2)
				recordLat(elapsed)
			}
		}(int64(w) + time.Now().UnixNano())
	}
	wg.Wait()

	return Result{
		Sent:        sent.Load(),
		Accepted:    accepted.Load(),
		Rejected:    rejected.Load(),
		NetworkErrs: nerr.Load(),
		Latencies:   lats,
	}, nil
}

func buildBatch(rng *rand.Rand, cfg Config) []model.Heartbeat {
	now := time.Now().UnixMilli()
	out := make([]model.Heartbeat, cfg.BatchSize)
	statuses := []model.Status{model.StatusOK, model.StatusOK, model.StatusOK, model.StatusWarn, model.StatusError}
	for i := 0; i < cfg.BatchSize; i++ {
		out[i] = model.Heartbeat{
			DeviceID:    fmt.Sprintf("dev-%d", rng.Intn(cfg.DevicePool)),
			Region:      cfg.Regions[rng.Intn(len(cfg.Regions))],
			Status:      statuses[rng.Intn(len(statuses))],
			TimestampMs: now,
			// Latency distribution: mostly low, occasional spikes. Lets
			// downstream p95/p99 be non-trivial.
			LatencyMs: expLatency(rng),
			CPUUsage:  rng.Float64(),
			MemUsage:  rng.Float64(),
		}
	}
	return out
}

// expLatency mixes an exponential baseline with a 1% spike tail, giving
// a realistic-looking IoT latency shape.
func expLatency(rng *rand.Rand) float64 {
	if rng.Intn(100) == 0 {
		return 500 + rng.Float64()*1500 // fat tail 500..2000 ms
	}
	return rng.ExpFloat64() * 20 // mean ~20ms
}

// postBatch submits a batch (or single) heartbeat and parses the
// ingestion service response.
func postBatch(ctx context.Context, cfg Config, batch []model.Heartbeat) (time.Duration, uint64, uint64, error) {
	var body []byte
	var path string
	if len(batch) == 1 {
		b, _ := json.Marshal(batch[0])
		body = b
		path = "/api/v1/heartbeat"
	} else {
		b, _ := json.Marshal(model.BatchRequest{Heartbeats: batch})
		body = b
		path = "/api/v1/heartbeats:batch"
	}
	req, err := http.NewRequestWithContext(ctx, "POST", cfg.Target+path, bytes.NewReader(body))
	if err != nil {
		return 0, 0, uint64(len(batch)), err
	}
	req.Header.Set("Content-Type", "application/json")

	start := time.Now()
	resp, err := cfg.HTTPClient.Do(req)
	elapsed := time.Since(start)
	if err != nil {
		return elapsed, 0, uint64(len(batch)), err
	}
	defer func() { io.Copy(io.Discard, resp.Body); resp.Body.Close() }()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return elapsed, uint64(len(batch)), 0, nil
	}
	return elapsed, 0, uint64(len(batch)), nil
}

func sortDurations(ds []time.Duration) {
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
}
