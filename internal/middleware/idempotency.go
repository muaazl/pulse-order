package middleware

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ============================================================================
// ARCHITECTURAL TEACHING: DISTRIBUTED IDEMPOTENCY GUARD (`SET ... NX EX`)
// ============================================================================
// WHY IDEMPOTENCY KEYS ARE ESSENTIAL IN PAYMENT & ORDER GATEWAYS:
// On mobile networks, a client may send `POST /api/v1/orders`, the server
// accepts the order and publishes `OrderCreatedEvent` to Kafka, and then the
// TCP connection drops before the client receives the `202 Accepted` response.
// The client SDK automatically retries the exact same POST request.
//
// Without an Idempotency Guard, the customer gets double-charged and two
// duplicate orders enter the Saga pipeline!
//
// HOW THIS MIDDLEWARE WORKS ATOMICALLY:
// 1. Require `Idempotency-Key` header on `POST /api/v1/orders`.
// 2. Execute `SET idemp:{key} "IN_PROGRESS" NX EX 300`:
//    - If `SET NX` succeeds (returns true), this goroutine owns the lock and
//      proceeds to `OrderBuilder` + Kafka publish.
//    - If `SET NX` returns false, the key already exists in Redis:
//      a) If the stored value is `"IN_PROGRESS"`, a concurrent duplicate
//         request is still executing -> return `409 Conflict`.
//      b) If the stored value is a serialized response payload, return the
//         exact cached `202 Accepted` JSON payload immediately with header
//         `X-Idempotency-Replayed: true` without touching Kafka!

const idempotencyInProgress = "IN_PROGRESS"

type cachedIdempotentResponse struct {
	StatusCode int             `json:"status_code"`
	Body       json.RawMessage `json:"body"`
}

// IdempotencyGuard enforces single-execution semantics for mutating order endpoints.
type IdempotencyGuard struct {
	client *redis.Client
	ttl    time.Duration
}

// NewIdempotencyGuard creates an IdempotencyGuard with the specified key retention TTL.
func NewIdempotencyGuard(client *redis.Client, ttl time.Duration) *IdempotencyGuard {
	if ttl <= 0 {
		ttl = 300 * time.Second
	}
	return &IdempotencyGuard{
		client: client,
		ttl:    ttl,
	}
}

// Wrap intercepts HTTP POST requests, enforces `Idempotency-Key`, and caches `2xx` responses.
func (ig *IdempotencyGuard) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}

		idemKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if idemKey == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error":   "missing_idempotency_key",
				"message": "Idempotency-Key header is required on POST /api/v1/orders",
			})
			return
		}

		redisKey := "idemp:" + idemKey
		ctx := r.Context()

		// Step 1: Attempt atomic lock acquisition (`SET key "IN_PROGRESS" NX EX 300`)
		acquired, err := ig.client.SetNX(ctx, redisKey, idempotencyInProgress, ig.ttl).Result()
		if err != nil {
			// If Redis is unreachable, fail open and process the request
			next.ServeHTTP(w, r)
			return
		}

		if !acquired {
			// Key already exists! Inspect current state in Redis.
			existingVal, getErr := ig.client.Get(ctx, redisKey).Result()
			if getErr == nil {
				if existingVal == idempotencyInProgress {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusConflict)
					_ = json.NewEncoder(w).Encode(map[string]string{
						"error":   "idempotency_key_in_progress",
						"message": "a request with this Idempotency-Key is currently being processed",
					})
					return
				}

				var cached cachedIdempotentResponse
				if unmarshalErr := json.Unmarshal([]byte(existingVal), &cached); unmarshalErr == nil {
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("X-Idempotency-Replayed", "true")
					w.WriteHeader(cached.StatusCode)
					_, _ = w.Write(cached.Body)
					return
				}
			}
		}

		// Step 2: Capture downstream response via a recording ResponseWriter
		rec := &responseRecorder{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
			body:           &bytes.Buffer{},
		}

		next.ServeHTTP(rec, r)

		// Step 3: If handler succeeded (2xx), persist the completed response under the idempotency key.
		// If handler failed with 4xx/5xx (e.g., 400 Bad Request from OrderBuilder), delete the lock
		// so the client can fix the payload or retry cleanly.
		if rec.statusCode >= 200 && rec.statusCode < 300 {
			snapshot, mErr := json.Marshal(cachedIdempotentResponse{
				StatusCode: rec.statusCode,
				Body:       json.RawMessage(rec.body.Bytes()),
			})
			if mErr == nil {
				_ = ig.client.Set(ctx, redisKey, snapshot, ig.ttl).Err()
			}
		} else {
			_ = ig.client.Del(ctx, redisKey).Err()
		}
	})
}

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	body       *bytes.Buffer
}

func (r *responseRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}

