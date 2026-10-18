package observability_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"pulseorder/internal/observability"
)

func TestObservability_PrometheusMetricsAndPprofEndpoints(t *testing.T) {
	metrics := observability.NewMetrics()
	mux := http.NewServeMux()

	mux.Handle("GET /metrics", metrics.Handler())
	observability.RegisterPprofRoutes(mux)

	mux.HandleFunc("POST /api/v1/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Simulate-429") == "true" {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	})

	handler := metrics.InstrumentHTTP(mux)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	// 1. Trigger a 202 Accepted and a 429 Too Many Requests
	resp202, err := http.Post(ts.URL+"/api/v1/orders", "application/json", strings.NewReader(`{}`))
	if err != nil || resp202.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %v", err)
	}
	_ = resp202.Body.Close()

	req429, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/orders", strings.NewReader(`{}`))
	req429.Header.Set("X-Simulate-429", "true")
	resp429, err := http.DefaultClient.Do(req429)
	if err != nil || resp429.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %v", err)
	}
	_ = resp429.Body.Close()

	// 2. Record Saga outcomes & Surge gauge
	metrics.RecordSagaOutcome("completed")
	metrics.RecordSagaOutcome("compensated_rollback")
	metrics.SetZoneSurge("zone-downtown", 2.0)

	// 3. Scrape /metrics and verify all required Prometheus series exist
	respMetrics, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer func() { _ = respMetrics.Body.Close() }()
	bodyBytes, _ := io.ReadAll(respMetrics.Body)
	metricsText := string(bodyBytes)

	requiredSeries := []string{
		`pulse_http_requests_total{method="POST",path="/api/v1/orders",status_code="202"} 1`,
		`pulse_http_requests_total{method="POST",path="/api/v1/orders",status_code="429"} 1`,
		`pulse_http_request_duration_seconds_bucket`,
		`pulse_saga_executions_total{outcome="completed"} 1`,
		`pulse_saga_executions_total{outcome="compensated_rollback"} 1`,
		`pulse_active_surge_multiplier{zone_id="zone-downtown"} 2`,
		`go_gc_duration_seconds`,
	}

	for _, expected := range requiredSeries {
		if !strings.Contains(metricsText, expected) {
			t.Errorf("missing expected Prometheus metric %q in /metrics output", expected)
		}
	}

	// 4. Verify /debug/pprof/ and /debug/pprof/heap respond with 200 OK
	respPprof, err := http.Get(ts.URL + "/debug/pprof/")
	if err != nil || respPprof.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /debug/pprof/, got %v", err)
	}
	_ = respPprof.Body.Close()

	respHeap, err := http.Get(ts.URL + "/debug/pprof/heap")
	if err != nil || respHeap.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /debug/pprof/heap, got %v", err)
	}
	_ = respHeap.Body.Close()
}

