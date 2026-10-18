package observability

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"pulseorder/internal/domain"
)

// ============================================================================
// ARCHITECTURAL TEACHING: PROMETHEUS RED METRICS & CARDINALITY DISCIPLINE
// ============================================================================
// In high-concurrency observability, we instrument services using the RED method:
//   - Rate:     `pulse_http_requests_total` (requests per second by status code)
//   - Errors:   `429` rate limits, `400` bad requests, `5xx` errors, and
//               `pulse_saga_executions_total{outcome="compensated_rollback"}`
//   - Duration: `pulse_http_request_duration_seconds` (Histogram buckets for
//               computing p50, p95, and p99 tail latencies via `histogram_quantile`)
//
// CARDINALITY GUARDRAIL:
// Notice that `normalizeRoutePath(r.URL.Path)` collapses dynamic path segments
// like `/api/v1/orders/ord-1729-abc` into `/api/v1/orders/{id}` BEFORE recording
// the Prometheus label. Never put unbounded IDs (`order_id`, `user_id`) into
// Prometheus labels, or a 1M-order spike will explode time-series cardinality
// and OOM the Prometheus TSDB!

// Metrics holds all registered Prometheus collectors for PulseOrder.
type Metrics struct {
	Registry            *prometheus.Registry
	HTTPRequestsTotal   *prometheus.CounterVec
	HTTPRequestDuration *prometheus.HistogramVec
	SagaExecutionsTotal *prometheus.CounterVec
	ActiveSurgeGauge    *prometheus.GaugeVec
}

// NewMetrics creates a dedicated Prometheus Registry pre-populated with Go runtime
// collectors (`go_gc_duration_seconds`, goroutines, memory) and PulseOrder custom metrics.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	// Register standard Go runtime (GC pauses, goroutines, heap) & process metrics
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	httpReqs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pulse_http_requests_total",
			Help: "Total HTTP requests processed by the PulseOrder API Gateway, partitioned by method, path, and status_code.",
		},
		[]string{"method", "path", "status_code"},
	)

	httpDur := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "pulse_http_request_duration_seconds",
			Help:    "HTTP request latency distribution in seconds (p50, p95, p99).",
			Buckets: []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5},
		},
		[]string{"method", "path"},
	)

	sagaExecs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "pulse_saga_executions_total",
			Help: "Total Saga orchestrations executed, partitioned by outcome (completed vs compensated_rollback).",
		},
		[]string{"outcome"},
	)

	surgeGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "pulse_active_surge_multiplier",
			Help: "Current real-time surge pricing multiplier per marketplace zone_id.",
		},
		[]string{"zone_id"},
	)

	reg.MustRegister(httpReqs, httpDur, sagaExecs, surgeGauge)

	// Pre-initialize known labels so Prometheus series exist immediately at startup
	sagaExecs.WithLabelValues("completed").Add(0)
	sagaExecs.WithLabelValues("compensated_rollback").Add(0)
	for zone := range domain.ValidZones {
		surgeGauge.WithLabelValues(zone).Set(1.0)
	}

	return &Metrics{
		Registry:            reg,
		HTTPRequestsTotal:   httpReqs,
		HTTPRequestDuration: httpDur,
		SagaExecutionsTotal: sagaExecs,
		ActiveSurgeGauge:    surgeGauge,
	}
}

// Handler returns the HTTP handler for `GET /metrics`.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

// InstrumentHTTP wraps an http.Handler to record `pulse_http_requests_total`
// and `pulse_http_request_duration_seconds` for every request (including `429` rate limits).
func (m *Metrics) InstrumentHTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" || strings.HasPrefix(r.URL.Path, "/debug/pprof") {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		sw := &statusResponseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)

		routeLabel := normalizeRoutePath(r.URL.Path)
		statusStr := strconv.Itoa(sw.status)
		elapsed := time.Since(start).Seconds()

		m.HTTPRequestsTotal.WithLabelValues(r.Method, routeLabel, statusStr).Inc()
		m.HTTPRequestDuration.WithLabelValues(r.Method, routeLabel).Observe(elapsed)
	})
}

// RecordSagaOutcome increments `pulse_saga_executions_total{outcome="..."}`.
func (m *Metrics) RecordSagaOutcome(outcome string) {
	m.SagaExecutionsTotal.WithLabelValues(outcome).Inc()
}

// SetZoneSurge updates `pulse_active_surge_multiplier{zone_id="..."}`.
func (m *Metrics) SetZoneSurge(zoneID string, multiplier float64) {
	m.ActiveSurgeGauge.WithLabelValues(strings.ToLower(strings.TrimSpace(zoneID))).Set(multiplier)
}

type statusResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusResponseWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// normalizeRoutePath prevents high-cardinality label explosion by collapsing
// dynamic resource IDs into canonical route templates.
func normalizeRoutePath(path string) string {
	switch {
	case path == "/api/v1/orders/search":
		return "/api/v1/orders/search"
	case strings.HasPrefix(path, "/api/v1/orders/"):
		return "/api/v1/orders/{id}"
	case strings.HasPrefix(path, "/api/v1/surge/"):
		return "/api/v1/surge/{zone_id}"
	default:
		return path
	}
}

