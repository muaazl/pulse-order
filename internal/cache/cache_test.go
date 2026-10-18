package cache_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"pulseorder/internal/cache"
	"pulseorder/internal/domain"
)

func newTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(func() { mr.Close() })

	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func TestRedisStore_AtomicInventoryReserveAndRelease(t *testing.T) {
	rc := newTestRedisClient(t)
	store := cache.NewRedisStoreFromClient(rc)
	ctx := context.Background()

	catalog := domain.DefaultCatalog()
	if err := store.SeedCatalog(ctx, catalog); err != nil {
		t.Fatalf("SeedCatalog failed: %v", err)
	}

	// TECH-MBP-14 has initial_stock = 50 in catalog_data.json
	items := []domain.OrderItem{
		{SKU: "TECH-MBP-14", Name: "MacBook Pro 14-inch M3", Quantity: 10, UnitPrice: 1999.00},
		{SKU: "TECH-AIRPODS-PRO", Name: "AirPods Pro", Quantity: 5, UnitPrice: 249.00},
	}

	if err := store.ReserveItems(ctx, items); err != nil {
		t.Fatalf("expected ReserveItems to succeed, got: %v", err)
	}

	stocks, err := store.GetSKUStockMap(ctx, []string{"TECH-MBP-14", "TECH-AIRPODS-PRO"})
	if err != nil {
		t.Fatalf("GetSKUStockMap failed: %v", err)
	}
	if stocks["TECH-MBP-14"] != 40 {
		t.Errorf("expected remaining stock 40 for TECH-MBP-14, got %d", stocks["TECH-MBP-14"])
	}
	if stocks["TECH-AIRPODS-PRO"] != 195 {
		t.Errorf("expected remaining stock 195 for TECH-AIRPODS-PRO, got %d", stocks["TECH-AIRPODS-PRO"])
	}

	// Attempt to reserve more than available (45 > 40) in a multi-SKU order:
	// Neither TECH-MBP-14 nor TECH-AIRPODS-PRO should be decremented!
	excessItems := []domain.OrderItem{
		{SKU: "TECH-AIRPODS-PRO", Quantity: 10, UnitPrice: 249.00},
		{SKU: "TECH-MBP-14", Quantity: 45, UnitPrice: 1999.00},
	}
	err = store.ReserveItems(ctx, excessItems)
	if !errors.Is(err, cache.ErrInsufficientStock) {
		t.Fatalf("expected ErrInsufficientStock, got: %v", err)
	}

	stocksAfterFail, _ := store.GetSKUStockMap(ctx, []string{"TECH-MBP-14", "TECH-AIRPODS-PRO"})
	if stocksAfterFail["TECH-AIRPODS-PRO"] != 195 || stocksAfterFail["TECH-MBP-14"] != 40 {
		t.Fatalf("atomic multi-SKU Lua check failed: stocks changed on failed reservation: %+v", stocksAfterFail)
	}

	// Release the first reservation (compensating transaction) -> stock returns to 50 and 200
	if err := store.ReleaseItems(ctx, items); err != nil {
		t.Fatalf("ReleaseItems failed: %v", err)
	}
	stocksRestored, _ := store.GetSKUStockMap(ctx, []string{"TECH-MBP-14", "TECH-AIRPODS-PRO"})
	if stocksRestored["TECH-MBP-14"] != 50 || stocksRestored["TECH-AIRPODS-PRO"] != 200 {
		t.Fatalf("expected restored stocks 50 and 200, got %+v", stocksRestored)
	}
}

func TestSurgeCache_SingleflightStampedePrevention(t *testing.T) {
	rc := newTestRedisClient(t)
	ctx := context.Background()

	// Simulate an expensive fallback calculator that takes 40ms
	sc := cache.NewSurgeCache(rc, func(_ context.Context, _ string) (float64, error) {
		time.Sleep(40 * time.Millisecond)
		return 1.60, nil
	})

	const concurrentCallers = 50
	var wg sync.WaitGroup
	wg.Add(concurrentCallers)

	for i := 0; i < concurrentCallers; i++ {
		go func() {
			defer wg.Done()
			mult, _, err := sc.GetZoneMultiplier(ctx, "zone-downtown")
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if mult != 1.60 {
				t.Errorf("expected multiplier 1.60, got %.2f", mult)
			}
		}()
	}

	wg.Wait()

	if execs := sc.FallbackExecutionCount(); execs != 1 {
		t.Fatalf("cache stampede occurred: expected fallback to execute exactly 1 time for 50 concurrent misses, got %d", execs)
	}
}

