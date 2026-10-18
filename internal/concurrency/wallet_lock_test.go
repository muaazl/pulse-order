package concurrency_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"pulseorder/internal/concurrency"
	"pulseorder/internal/domain"
)

func TestWalletManager_BidirectionalConcurrentTransfers_NoDeadlock(t *testing.T) {
	wm := concurrency.NewWalletManager(domain.DefaultCatalog().Wallets)
	ctx := context.Background()

	const workersPerDirection = 100
	var wg sync.WaitGroup
	wg.Add(workersPerDirection * 2)

	// 100 goroutines transferring user-alice -> user-bob ($5.00)
	// 100 goroutines transferring user-bob -> user-alice ($5.00)
	// Without deterministic lock ordering, this collides in an AB-BA deadlock immediately.
	for i := 0; i < workersPerDirection; i++ {
		go func() {
			defer wg.Done()
			receipt, err := wm.Transfer(ctx, "user-alice", "user-bob", 5.00)
			if err != nil {
				t.Errorf("unexpected error alice->bob: %v", err)
				return
			}
			if receipt.LockOrder[0] != "user-alice" || receipt.LockOrder[1] != "user-bob" {
				t.Errorf("expected deterministic lock order [user-alice, user-bob], got %v", receipt.LockOrder)
			}
		}()

		go func() {
			defer wg.Done()
			receipt, err := wm.Transfer(ctx, "user-bob", "user-alice", 5.00)
			if err != nil {
				t.Errorf("unexpected error bob->alice: %v", err)
				return
			}
			// Notice that even when transferring bob -> alice, lock order MUST be [user-alice, user-bob]!
			if receipt.LockOrder[0] != "user-alice" || receipt.LockOrder[1] != "user-bob" {
				t.Errorf("expected deterministic lock order [user-alice, user-bob], got %v", receipt.LockOrder)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("DEADLOCK DETECTED: concurrent bidirectional wallet transfers timed out")
	}

	balances, err := wm.SnapshotBalances(ctx)
	if err != nil {
		t.Fatalf("SnapshotBalances failed: %v", err)
	}

	// Both started at $5000.00 and exchanged equal amounts 100 times each
	if balances["user-alice"] != 5000.00 || balances["user-bob"] != 5000.00 {
		t.Fatalf("money conservation violated: alice=%.2f bob=%.2f", balances["user-alice"], balances["user-bob"])
	}
}

func TestWalletManager_LockTimeout500ms(t *testing.T) {
	wm := concurrency.NewWalletManager(domain.DefaultCatalog().Wallets)
	ctx := context.Background()

	// Hold lock on user-alice manually via AcquireMultipleLocks
	release, _, err := wm.AcquireMultipleLocks(ctx, []string{"user-alice"})
	if err != nil {
		t.Fatalf("unexpected lock error: %v", err)
	}
	defer release()

	// Now attempt a transfer involving user-alice; it must time out within ~500ms
	start := time.Now()
	_, err = wm.Transfer(ctx, "user-bob", "user-alice", 10.00)
	elapsed := time.Since(start)

	if !errors.Is(err, concurrency.ErrLockTimeout) {
		t.Fatalf("expected ErrLockTimeout, got: %v", err)
	}
	if elapsed < 450*time.Millisecond || elapsed > 1500*time.Millisecond {
		t.Errorf("expected lock timeout around 500ms, took %v", elapsed)
	}
}

