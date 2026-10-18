package cache

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// ============================================================================
// ARCHITECTURAL TEACHING: CACHE STAMPEDE (THUNDERING HERD) PREVENTION
// ============================================================================
// THE PROBLEM:
// Suppose `surge:zone:zone-downtown` has a 120-second TTL in Redis. At the exact
// millisecond the key expires, 1,000 concurrent HTTP requests arrive for
// `zone-downtown`. All 1,000 goroutines see `redis.Nil` (cache miss) and
// simultaneously invoke the fallback calculator / database / Redis write,
// causing a CPU & I/O spike known as a "Cache Stampede" or "Thundering Herd".
//
// THE SOLUTION (`golang.org/x/sync/singleflight`):
// `singleflight.Group.Do(key, fn)` coalesces concurrent in-flight calls for the
// same key. When 1,000 goroutines miss the Redis cache simultaneously:
//   - Goroutine #1 executes `fn` (computes fallback multiplier & writes to Redis).
//   - Goroutines #2..#1000 park on an in-memory waitgroup and receive the exact
//     same return value (`1.0x` or computed multiplier) with ZERO redundant
//     downstream calls.

// FallbackCalculator computes a zone's surge multiplier when the Redis key has expired.
type FallbackCalculator func(ctx context.Context, zoneID string) (float64, error)

// SurgeCache manages real-time zone surge multipliers backed by Redis + singleflight.
type SurgeCache struct {
	client             *redis.Client
	sf                 singleflight.Group
	fallback           FallbackCalculator
	fallbackExecutions atomic.Int64 // Exported viaFallbackCount() to prove singleflight in tests
}

// NewSurgeCache constructs a SurgeCache with an optional fallback calculator.
// If fallback is nil, a safe default (1.0x neutral multiplier) is used on cache miss.
func NewSurgeCache(client *redis.Client, fallback FallbackCalculator) *SurgeCache {
	if fallback == nil {
		fallback = func(_ context.Context, _ string) (float64, error) {
			return 1.0, nil
		}
	}
	return &SurgeCache{
		client:   client,
		fallback: fallback,
	}
}

func surgeKey(zoneID string) string {
	return "surge:zone:" + strings.ToLower(strings.TrimSpace(zoneID))
}

// GetZoneMultiplier reads `surge:zone:{zone_id}` from Redis. On cache miss,
// it uses singleflight to prevent cache stampedes while repopulating Redis.
func (sc *SurgeCache) GetZoneMultiplier(ctx context.Context, zoneID string) (float64, bool, error) {
	normalizedZone := strings.ToLower(strings.TrimSpace(zoneID))
	key := surgeKey(normalizedZone)

	// Fast path: direct Redis GET
	valStr, err := sc.client.Get(ctx, key).Result()
	if err == nil {
		parsed, parseErr := strconv.ParseFloat(valStr, 64)
		if parseErr == nil && parsed >= 1.0 {
			return parsed, false, nil
		}
	} else if !errors.Is(err, redis.Nil) {
		// If Redis is transiently unreachable, degrade gracefully to 1.0x
		return 1.0, false, err
	}

	// Slow path (Cache Miss): Coalesce concurrent requests via singleflight.Do
	val, err, shared := sc.sf.Do(normalizedZone, func() (any, error) {
		// Double-check Redis inside singleflight in case an earlier flight just finished
		if cachedStr, getErr := sc.client.Get(ctx, key).Result(); getErr == nil {
			if parsed, pErr := strconv.ParseFloat(cachedStr, 64); pErr == nil && parsed >= 1.0 {
				return parsed, nil
			}
		}

		sc.fallbackExecutions.Add(1)
		computed, calcErr := sc.fallback(ctx, normalizedZone)
		if calcErr != nil || computed < 1.0 {
			computed = 1.0
		}

		// Populate Redis with 120s TTL (matching Flink sink TTL in PRD 3.7)
		_ = sc.SetZoneMultiplier(ctx, normalizedZone, computed, 120*time.Second)
		return computed, nil
	})

	if err != nil {
		return 1.0, shared, err
	}
	return val.(float64), shared, nil
}

// SetZoneMultiplier writes a zone's surge multiplier to Redis (`SET surge:zone:{zone_id} {multiplier} EX 120`).
func (sc *SurgeCache) SetZoneMultiplier(ctx context.Context, zoneID string, multiplier float64, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 120 * time.Second
	}
	formatted := fmt.Sprintf("%.2f", multiplier)
	return sc.client.Set(ctx, surgeKey(zoneID), formatted, ttl).Err()
}

// FallbackExecutionCount returns how many times the underlying fallback function
// was actually executed (used in concurrency tests to verify singleflight coalescing).
func (sc *SurgeCache) FallbackExecutionCount() int64 {
	return sc.fallbackExecutions.Load()
}

