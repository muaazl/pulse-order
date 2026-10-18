package middleware

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// ============================================================================
// ARCHITECTURAL TEACHING: REDIS SLIDING-WINDOW RATE LIMITING (LUA SCRIPT)
// ============================================================================
// WHY SLIDING WINDOW INSTEAD OF FIXED WINDOW?
// A Fixed-Window counter (`INCR ratelimit:user:1729000010`) suffers from the
// "Boundary Burst Problem": if the limit is 10 req / 10s, a client can send 10
// requests at t=9.9s and another 10 requests at t=10.1s — pushing 20 requests
// through in 200ms (2x the downstream capacity).
//
// WHY A SINGLE REDIS LUA SCRIPT?
// Implementing a Sliding Window with separate Go round-trips (`ZREMRANGEBYSCORE`,
// then `ZCARD`, then `ZADD`) creates a race condition: 50 concurrent goroutines
// from the same client will all read `ZCARD == 9` before any of their `ZADD`
// calls execute, allowing all 50 requests through!
//
// By embedding the entire sliding-window algorithm inside a single Redis Lua
// script, Redis executes `ZREMRANGEBYSCORE -> ZCARD -> ZADD -> PEXPIRE`
// atomically on its single-threaded event loop:
//   1. Evict all timestamps older than `now_ms - window_ms` (`ZREMRANGEBYSCORE`).
//   2. Count active requests inside the sliding window (`ZCARD`).
//   3. If `count >= limit`, reject immediately (`429 Too Many Requests`) and
//      return the oldest entry's timestamp so we can compute an accurate
//      `Retry-After` header.
//   4. Otherwise, insert the current request member (`ZADD`), refresh key TTL
//      (`PEXPIRE`), and return the remaining quota.
var slidingWindowLua = redis.NewScript(`
local key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

local window_start = now_ms - window_ms
redis.call("ZREMRANGEBYSCORE", key, "-inf", window_start)

local current_count = redis.call("ZCARD", key)
if current_count >= limit then
    local oldest = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
    local retry_ms = window_ms
    if oldest ~= nil and #oldest >= 2 then
        local oldest_ts = tonumber(oldest[2])
        retry_ms = math.max(100, (oldest_ts + window_ms) - now_ms)
    end
    return {0, 0, retry_ms}
end

redis.call("ZADD", key, now_ms, member)
redis.call("PEXPIRE", key, window_ms)
local remaining = limit - (current_count + 1)
return {1, remaining, 0}
`)

// SlidingWindowRateLimiter enforces per-client sliding-window rate limits backed by Redis.
type SlidingWindowRateLimiter struct {
	client *redis.Client
	limit  int
	window time.Duration
	seq    atomic.Uint64
}

// NewSlidingWindowRateLimiter creates a rate limiter enforcing `limit` requests per `window`.
func NewSlidingWindowRateLimiter(client *redis.Client, limit int, window time.Duration) *SlidingWindowRateLimiter {
	return &SlidingWindowRateLimiter{
		client: client,
		limit:  limit,
		window: window,
	}
}

// Wrap returns an http.Handler middleware that enforces the sliding-window rate limit.
func (rl *SlidingWindowRateLimiter) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		subject := extractClientIdentity(r)
		redisKey := "ratelimit:sw:" + subject

		nowMs := time.Now().UTC().UnixMilli()
		windowMs := rl.window.Milliseconds()
		uniqueMember := fmt.Sprintf("%d-%d", nowMs, rl.seq.Add(1))

		res, err := slidingWindowLua.Run(
			r.Context(),
			rl.client,
			[]string{redisKey},
			nowMs,
			windowMs,
			rl.limit,
			uniqueMember,
		).Slice()

		// Fail-open if Redis is temporarily unavailable so the gateway doesn't brick
		if err != nil {
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(rl.limit))
			next.ServeHTTP(w, r)
			return
		}

		allowed, _ := res[0].(int64)
		remaining, _ := res[1].(int64)
		retryMs, _ := res[2].(int64)

		w.Header().Set("X-RateLimit-Limit", strconv.Itoa(rl.limit))
		w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(remaining, 10))

		if allowed == 0 {
			retryAfterSec := int(math.Ceil(float64(retryMs) / 1000.0))
			if retryAfterSec < 1 {
				retryAfterSec = 1
			}
			w.Header().Set("Retry-After", strconv.Itoa(retryAfterSec))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error":               "rate_limit_exceeded",
				"message":             fmt.Sprintf("sliding window rate limit of %d requests per %s exceeded for %q", rl.limit, rl.window, subject),
				"retry_after_seconds": retryAfterSec,
			})
			return
		}

		next.ServeHTTP(w, r)
	})
}

// extractClientIdentity resolves the rate-limit subject from `X-User-ID` header
// or falls back to the caller's IP address.
func extractClientIdentity(r *http.Request) string {
	if uid := strings.TrimSpace(r.Header.Get("X-User-ID")); uid != "" {
		return "user:" + uid
	}
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		parts := strings.Split(xff, ",")
		return "ip:" + strings.TrimSpace(parts[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && host != "" {
		return "ip:" + host
	}
	if r.RemoteAddr != "" {
		return "ip:" + r.RemoteAddr
	}
	return "ip:anonymous"
}

