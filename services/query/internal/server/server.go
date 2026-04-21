// Package server exposes the operator dashboard API.
//
// Endpoints:
//
//	GET /api/v1/metrics         - time-series aggregates
//	GET /api/v1/distinct        - distinct device count over a window
//	GET /api/v1/dashboard       - composite view (aggregates + distinct + totals)
//
// All endpoints accept the same filter set: from, to, region, status.
// Time arguments are unix millis; we pick ms (not seconds) to match
// Flink event-time resolution.
package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/airwallex/heartbeat/pkg/chx"
	"github.com/airwallex/heartbeat/pkg/model"
)

const (
	maxQueryWindow = 7 * 24 * time.Hour
	defaultLimit   = 2000
	maxLimit       = 10000
)

type Options struct {
	Runner chx.QueryRunner
	Logger *slog.Logger
	Now    func() time.Time
}

type Server struct{ opts Options }

func New(o Options) *Server {
	if o.Runner == nil {
		panic("server: Runner is required")
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Server{opts: o}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /api/v1/metrics", s.handleMetrics)
	mux.HandleFunc("GET /api/v1/distinct", s.handleDistinct)
	mux.HandleFunc("GET /api/v1/dashboard", s.handleDashboard)
	return mux
}

// parseFilter extracts and normalises query parameters. The heavy lifting
// lives here so handlers stay tiny and each validation branch is unit
// tested in isolation.
func (s *Server) parseFilter(r *http.Request) (chx.Filter, error) {
	q := r.URL.Query()
	var f chx.Filter

	fromS, toS := q.Get("from"), q.Get("to")
	if fromS == "" || toS == "" {
		return f, errors.New("from and to (unix ms) are required")
	}
	from, err := strconv.ParseInt(fromS, 10, 64)
	if err != nil {
		return f, fmt.Errorf("invalid from: %w", err)
	}
	to, err := strconv.ParseInt(toS, 10, 64)
	if err != nil {
		return f, fmt.Errorf("invalid to: %w", err)
	}
	if to <= from {
		return f, errors.New("to must be greater than from")
	}
	if time.Duration(to-from)*time.Millisecond > maxQueryWindow {
		return f, fmt.Errorf("query window exceeds max of %s", maxQueryWindow)
	}

	f.FromMs = from
	f.ToMs = to
	f.Region = q.Get("region")
	f.Status = q.Get("status")
	// device_id is intentionally not a filter here - see chx.Filter godoc.
	// If a client sends it we reject the request rather than silently
	// returning unfiltered data (which was the behavior before we
	// removed the dead field).
	if q.Get("device_id") != "" {
		return f, errors.New("device_id filter is not supported; query the raw heartbeats table directly")
	}

	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n <= 0 {
			return f, fmt.Errorf("invalid limit")
		}
		if n > maxLimit {
			n = maxLimit
		}
		f.Limit = n
	} else {
		f.Limit = defaultLimit
	}

	if f.Status != "" && !model.Status(f.Status).IsValid() {
		return f, errors.New("status must be one of ok|warn|error")
	}
	return f, nil
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.opts.Runner.Ping(r.Context()); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	f, err := s.parseFilter(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	out, err := s.opts.Runner.FetchAggregates(r.Context(), f)
	if err != nil {
		s.opts.Logger.Error("fetch aggregates", "err", err)
		writeErr(w, http.StatusInternalServerError, errors.New("query failed"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"filter": f,
		"count":  len(out),
		"series": out,
	})
}

func (s *Server) handleDistinct(w http.ResponseWriter, r *http.Request) {
	f, err := s.parseFilter(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	n, err := s.opts.Runner.DistinctDevices(r.Context(), f)
	if err != nil {
		s.opts.Logger.Error("distinct", "err", err)
		writeErr(w, http.StatusInternalServerError, errors.New("query failed"))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"filter": f, "distinct_devices": n})
}

// handleDashboard composes the operator view. We roll up the aggregates
// into overall p95/p99/avg weighted by count, which is a coarse but
// intuitive summary. Exact global percentiles would require re-merging
// t-digests; if that's needed we'll store digest state in ClickHouse
// and compute it here.
func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	f, err := s.parseFilter(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	agg, err := s.opts.Runner.FetchAggregates(r.Context(), f)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("query failed"))
		return
	}
	distinct, err := s.opts.Runner.DistinctDevices(r.Context(), f)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, errors.New("query failed"))
		return
	}
	summary := rollup(agg)
	summary.DistinctDevices = distinct
	summary.Filter = f
	writeJSON(w, http.StatusOK, summary)
}

// DashboardView is the rolled-up response of /api/v1/dashboard.
type DashboardView struct {
	Filter          chx.Filter              `json:"filter"`
	TotalEvents     uint64                  `json:"total_events"`
	DistinctDevices uint64                  `json:"distinct_devices"`
	AvgLatencyMs    float64                 `json:"avg_latency_ms"`
	P95LatencyMs    float64                 `json:"p95_latency_ms_weighted"`
	P99LatencyMs    float64                 `json:"p99_latency_ms_weighted"`
	Series          []model.WindowAggregate `json:"series"`
}

func rollup(windows []model.WindowAggregate) DashboardView {
	var total uint64
	var sumAvg, sumP95, sumP99 float64
	for _, w := range windows {
		total += w.Count
		// count-weighted sums let us average without storing digests.
		sumAvg += w.AvgLatencyMs * float64(w.Count)
		sumP95 += w.P95LatencyMs * float64(w.Count)
		sumP99 += w.P99LatencyMs * float64(w.Count)
	}
	if total == 0 {
		return DashboardView{Series: windows}
	}
	t := float64(total)
	return DashboardView{
		TotalEvents:  total,
		AvgLatencyMs: sumAvg / t,
		P95LatencyMs: sumP95 / t,
		P99LatencyMs: sumP99 / t,
		Series:       windows,
	}
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]any{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
