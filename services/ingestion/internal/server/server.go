// Package server wires the HTTP handlers for the ingestion service.
//
// Design goals:
//   - Accept single and batch heartbeats.
//   - Validate early, reject cheap.
//   - Publish to Kafka synchronously per request (caller-driven
//     backpressure); Kafka writer itself batches under the hood.
package server

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/airwallex/heartbeat/pkg/kafkax"
	"github.com/airwallex/heartbeat/pkg/model"
)

const (
	defaultMaxBodyBytes = 1 << 20 // 1 MiB
	defaultMaxBatch     = 1000
	defaultClockSkew    = 2 * time.Minute
)

// Options configures Server. Zero values are replaced by sensible defaults.
type Options struct {
	Producer     kafkax.Producer
	Logger       *slog.Logger
	Now          func() time.Time
	MaxBodyBytes int64
	MaxBatch     int
	ClockSkew    time.Duration
}

// Server is the HTTP application. It exposes Handler() for wiring into any
// net/http compatible server.
type Server struct {
	opts Options

	// metrics kept in-process so /metrics can render without a Prometheus
	// dependency (this is a demo - swap for prometheus/client_golang in
	// production).
	//
	// accepted  - events successfully published to Kafka
	// rejected  - events dropped by validation/size/format checks
	// failures  - events that passed validation but could not be published
	//             (broker down, context cancelled, etc.)
	accepted atomic.Uint64
	rejected atomic.Uint64
	failures atomic.Uint64
}

// New builds a Server, panicking on obvious misconfiguration.
func New(o Options) *Server {
	if o.Producer == nil {
		panic("server: Producer is required")
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.MaxBodyBytes == 0 {
		o.MaxBodyBytes = defaultMaxBodyBytes
	}
	if o.MaxBatch == 0 {
		o.MaxBatch = defaultMaxBatch
	}
	if o.ClockSkew == 0 {
		o.ClockSkew = defaultClockSkew
	}
	return &Server{opts: o}
}

// Handler returns the HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/heartbeat", s.handleSingle)
	mux.HandleFunc("POST /api/v1/heartbeats:batch", s.handleBatch)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	return withRequestLog(s.opts.Logger, mux)
}

func (s *Server) handleSingle(w http.ResponseWriter, r *http.Request) {
	body, err := readLimited(r, s.opts.MaxBodyBytes)
	if err != nil {
		s.writeErr(w, http.StatusRequestEntityTooLarge, err)
		return
	}
	var h model.Heartbeat
	if err := json.Unmarshal(body, &h); err != nil {
		s.rejected.Add(1)
		s.writeErr(w, http.StatusBadRequest, err)
		return
	}
	if err := h.Validate(s.opts.Now(), s.opts.ClockSkew); err != nil {
		s.rejected.Add(1)
		s.writeErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	if err := s.opts.Producer.Publish(r.Context(), []model.Heartbeat{h}); err != nil {
		s.failures.Add(1)
		s.opts.Logger.Error("publish failed", "err", err)
		s.writeErr(w, http.StatusServiceUnavailable, errors.New("publish failed"))
		return
	}
	s.accepted.Add(1)
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": 1})
}

func (s *Server) handleBatch(w http.ResponseWriter, r *http.Request) {
	body, err := readLimited(r, s.opts.MaxBodyBytes)
	if err != nil {
		s.writeErr(w, http.StatusRequestEntityTooLarge, err)
		return
	}
	var req model.BatchRequest
	if err := json.Unmarshal(body, &req); err != nil {
		s.rejected.Add(1)
		s.writeErr(w, http.StatusBadRequest, err)
		return
	}
	if len(req.Heartbeats) == 0 {
		s.writeErr(w, http.StatusBadRequest, errors.New("heartbeats is empty"))
		return
	}
	if len(req.Heartbeats) > s.opts.MaxBatch {
		s.writeErr(w, http.StatusRequestEntityTooLarge, errors.New("batch too large"))
		return
	}

	// Partial validation: we reject the *whole* batch on any invalid
	// event. Callers can retry the good subset, and this preserves the
	// invariant that every published event is valid.
	now := s.opts.Now()
	for i := range req.Heartbeats {
		if err := req.Heartbeats[i].Validate(now, s.opts.ClockSkew); err != nil {
			s.rejected.Add(uint64(len(req.Heartbeats)))
			s.writeErr(w, http.StatusUnprocessableEntity, err)
			return
		}
	}
	if err := s.opts.Producer.Publish(r.Context(), req.Heartbeats); err != nil {
		s.failures.Add(uint64(len(req.Heartbeats)))
		s.opts.Logger.Error("publish failed", "err", err, "batch", len(req.Heartbeats))
		s.writeErr(w, http.StatusServiceUnavailable, errors.New("publish failed"))
		return
	}
	s.accepted.Add(uint64(len(req.Heartbeats)))
	writeJSON(w, http.StatusAccepted, map[string]any{"accepted": len(req.Heartbeats)})
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted_total":         s.accepted.Load(),
		"rejected_total":         s.rejected.Load(),
		"publish_failures_total": s.failures.Load(),
	})
}

func (s *Server) writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]any{"error": err.Error()})
}

func readLimited(r *http.Request, max int64) ([]byte, error) {
	r.Body = http.MaxBytesReader(nil, r.Body, max)
	return io.ReadAll(r.Body)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func withRequestLog(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: 200}
		next.ServeHTTP(sw, r)
		logger.Debug("http",
			"method", r.Method, "path", r.URL.Path,
			"status", sw.code, "elapsed_ms", time.Since(start).Milliseconds(),
		)
	})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (s *statusWriter) WriteHeader(c int) { s.code = c; s.ResponseWriter.WriteHeader(c) }
