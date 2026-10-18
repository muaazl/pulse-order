package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"pulseorder/internal/middleware"
)

func setupMiniredis(t *testing.T) *redis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(func() { mr.Close() })
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func TestSlidingWindowRateLimiter_EnforcesLimitAnd429Headers(t *testing.T) {
	rc := setupMiniredis(t)
	rl := middleware.NewSlidingWindowRateLimiter(rc, 10, 10*time.Second)

	handler := rl.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))

	// First 10 requests within 10s window must succeed (202 Accepted)
	for i := 1; i <= 10; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/orders", nil)
		req.Header.Set("X-User-ID", "user-burst-test")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusAccepted {
			t.Fatalf("request %d: expected 202 Accepted, got %d", i, rec.Code)
		}
		if rec.Header().Get("X-RateLimit-Limit") != "10" {
			t.Errorf("expected X-RateLimit-Limit=10, got %s", rec.Header().Get("X-RateLimit-Limit"))
		}
	}

	// 11th request must be rejected with 429 Too Many Requests + Retry-After header
	req11 := httptest.NewRequest(http.MethodPost, "/api/v1/orders", nil)
	req11.Header.Set("X-User-ID", "user-burst-test")
	rec11 := httptest.NewRecorder()
	handler.ServeHTTP(rec11, req11)

	if rec11.Code != http.StatusTooManyRequests {
		t.Fatalf("11th request: expected 429 Too Many Requests, got %d", rec11.Code)
	}
	if rec11.Header().Get("X-RateLimit-Remaining") != "0" {
		t.Errorf("expected X-RateLimit-Remaining=0, got %s", rec11.Header().Get("X-RateLimit-Remaining"))
	}
	if rec11.Header().Get("Retry-After") == "" {
		t.Errorf("expected non-empty Retry-After header on 429 response")
	}
}

func TestIdempotencyGuard_PreventsDuplicateExecution(t *testing.T) {
	rc := setupMiniredis(t)
	ig := middleware.NewIdempotencyGuard(rc, 5*time.Minute)

	var handlerCalls atomic.Int32
	handler := ig.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handlerCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"order_id":"ord-idemp-123","status":"ACCEPTED"}`))
	}))

	// 1. Missing Idempotency-Key -> 400 Bad Request
	reqNoKey := httptest.NewRequest(http.MethodPost, "/api/v1/orders", strings.NewReader(`{}`))
	recNoKey := httptest.NewRecorder()
	handler.ServeHTTP(recNoKey, reqNoKey)
	if recNoKey.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request when Idempotency-Key is missing, got %d", recNoKey.Code)
	}

	// 2. First call with Idempotency-Key -> executes handler (202 Accepted)
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/orders", strings.NewReader(`{}`))
	req1.Header.Set("Idempotency-Key", "idem-uuid-001")
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, req1)

	if rec1.Code != http.StatusAccepted {
		t.Fatalf("expected 202 Accepted on first request, got %d", rec1.Code)
	}
	if handlerCalls.Load() != 1 {
		t.Fatalf("expected handlerCalls == 1, got %d", handlerCalls.Load())
	}

	// 3. Second call with same Idempotency-Key -> returns cached 202 WITHOUT calling handler again
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/orders", strings.NewReader(`{}`))
	req2.Header.Set("Idempotency-Key", "idem-uuid-001")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)

	if rec2.Code != http.StatusAccepted {
		t.Fatalf("expected cached 202 Accepted on retry, got %d", rec2.Code)
	}
	if rec2.Header().Get("X-Idempotency-Replayed") != "true" {
		t.Errorf("expected X-Idempotency-Replayed: true header")
	}
	if handlerCalls.Load() != 1 {
		t.Fatalf("duplicate execution! expected handlerCalls to remain 1, got %d", handlerCalls.Load())
	}
}

func TestAdminAuthGuard_401And403Precision(t *testing.T) {
	guard := middleware.NewAdminAuthGuard("secret-key-123")
	handler := guard.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// 1. Missing API key -> 401 Unauthorized
	req401 := httptest.NewRequest(http.MethodPost, "/api/v1/admin/stock-reset", nil)
	rec401 := httptest.NewRecorder()
	handler.ServeHTTP(rec401, req401)
	if rec401.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 Unauthorized, got %d", rec401.Code)
	}

	// 2. Valid API key, but role is "viewer" -> 403 Forbidden
	req403 := httptest.NewRequest(http.MethodPost, "/api/v1/admin/stock-reset", nil)
	req403.Header.Set("X-API-Key", "secret-key-123")
	req403.Header.Set("X-Admin-Role", "viewer")
	rec403 := httptest.NewRecorder()
	handler.ServeHTTP(rec403, req403)
	if rec403.Code != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden, got %d", rec403.Code)
	}

	// 3. Valid API key + role "admin" -> 200 OK
	req200 := httptest.NewRequest(http.MethodPost, "/api/v1/admin/stock-reset", nil)
	req200.Header.Set("X-API-Key", "secret-key-123")
	req200.Header.Set("X-Admin-Role", "admin")
	rec200 := httptest.NewRecorder()
	handler.ServeHTTP(rec200, req200)
	if rec200.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec200.Code)
	}
}

