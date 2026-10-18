package cache_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"pulseorder/internal/cache"
)

func TestSlidingWindowSurgeEngine_DemandThresholdsAndWindowExpiration(t *testing.T) {
	rc := newTestRedisClient(t)
	ctx := context.Background()

	sc := cache.NewSurgeCache(rc, nil)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := cache.NewSlidingWindowSurgeEngine(
		sc,
		5*time.Minute,
		30*time.Second,
		logger,
	)

	baseTime := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

	// 1. Send 19 orders at t=12:00:00 -> < 20 orders => 1.0x
	var mult float64
	for i := 0; i < 19; i++ {
		mult, _, _ = engine.RecordOrder(ctx, "zone-downtown", baseTime)
	}
	if mult != 1.0 {
		t.Fatalf("expected 1.0x for 19 orders, got %.2f", mult)
	}

	// 2. Send 20th order at t=12:01:00 -> 20..50 orders => 1.3x
	mult, _, _ = engine.RecordOrder(ctx, "zone-downtown", baseTime.Add(1*time.Minute))
	if mult != 1.3 {
		t.Fatalf("expected 1.3x at 20 orders, got %.2f", mult)
	}

	// 3. Send 31 more orders at t=12:02:00 (total 51) -> 51..100 orders => 1.6x
	for i := 0; i < 31; i++ {
		mult, _, _ = engine.RecordOrder(ctx, "zone-downtown", baseTime.Add(2*time.Minute))
	}
	if mult != 1.6 {
		t.Fatalf("expected 1.6x at 51 orders, got %.2f", mult)
	}

	// 4. Send 50 more orders at t=12:03:00 (total 101) -> > 100 orders => 2.0x
	for i := 0; i < 50; i++ {
		mult, _, _ = engine.RecordOrder(ctx, "zone-downtown", baseTime.Add(3*time.Minute))
	}
	if mult != 2.0 {
		t.Fatalf("expected 2.0x at 101 orders, got %.2f", mult)
	}

	// Verify Redis cache holds "2.00" for `surge:zone:zone-downtown`
	cachedMult, _, err := sc.GetZoneMultiplier(ctx, "zone-downtown")
	if err != nil || cachedMult != 2.0 {
		t.Fatalf("expected Redis surge:zone:zone-downtown to be 2.0, got %.2f (err=%v)", cachedMult, err)
	}

	// 5. Advance sliding-window hop to t=12:07:30 (6.5 minutes after the first 51 orders,
	// leaving only the 50 orders from t=12:03:00 inside [12:02:30, 12:07:30]):
	// 50 active orders -> multiplier steps down smoothly from 2.0x to 1.3x!
	hopResults := engine.EvaluateAllZonesAt(ctx, baseTime.Add(7*time.Minute+30*time.Second))
	if hopResults["zone-downtown"] != 1.3 {
		t.Fatalf("expected sliding window decay to 1.3x (50 active orders remaining), got %.2f", hopResults["zone-downtown"])
	}

	// 6. Advance sliding-window hop to t=12:09:00 (all orders > 5 minutes old):
	// 0 active orders -> multiplier decays back to 1.0x!
	hopResultsFinal := engine.EvaluateAllZonesAt(ctx, baseTime.Add(9*time.Minute))
	if hopResultsFinal["zone-downtown"] != 1.0 {
		t.Fatalf("expected sliding window decay to 1.0x after 5m expiry, got %.2f", hopResultsFinal["zone-downtown"])
	}
}

