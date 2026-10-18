package concurrency

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"pulseorder/internal/domain"
)

// ============================================================================
// ARCHITECTURAL TEACHING: DEADLOCK PREVENTION (COFFMAN CONDITIONS IN GO)
// ============================================================================
// THE CLASSIC AB-BA DEADLOCK:
// Suppose Goroutine A executes `Transfer("user-bob", "user-alice", $50)` at the
// exact same nanosecond Goroutine B executes `Transfer("user-alice", "user-bob", $30)`:
//   - If we naively lock `fromAccount` then `toAccount`:
//     * Goroutine A locks `user-bob`, then blocks waiting for `user-alice`.
//     * Goroutine B locks `user-alice`, then blocks waiting for `user-bob`.
//     * Both goroutines block forever!
//
// HOW WE PREVENT DEADLOCKS IN HIGH-CONCURRENCY GO:
// 1. Deterministic Lock Ordering (Breaking Circular Wait):
//    Before acquiring any locks, we sort the involved account/SKU IDs in
//    strict ascending lexicographical order (`if firstID > secondID { swap }`).
//    Now BOTH Goroutine A and Goroutine B attempt to lock `"user-alice"` first
//    and `"user-bob"` second. A circular wait is mathematically impossible.
//
// 2. Bounded Lock Acquisition Timeouts (`500ms` via `chan struct{}`):
//    Go's standard `sync.Mutex.Lock()` does not accept a `context.Context`.
//    To enforce a strict `500ms` lock timeout (`context.WithTimeout`), each
//    account uses a 1-buffered channel semaphore (`sem chan struct{}`).
//    Acquiring the lock uses a `select` statement over `m.sem <- struct{}{}`
//    and `<-lockCtx.Done()`, guaranteeing no goroutine ever hangs indefinitely.

const DefaultLockTimeout = 500 * time.Millisecond

var (
	ErrSameAccount         = errors.New("cannot transfer between identical account IDs")
	ErrInvalidAmount       = errors.New("transfer amount must be strictly positive")
	ErrAccountNotFound     = errors.New("wallet account not found")
	ErrInsufficientBalance = errors.New("insufficient wallet balance")
	ErrLockTimeout         = errors.New("lock acquisition timed out after 500ms")
)

// timedLock is a context-aware mutex implemented with a 1-buffered channel.
type timedLock struct {
	sem chan struct{}
}

func newTimedLock() *timedLock {
	return &timedLock{sem: make(chan struct{}, 1)}
}

func (l *timedLock) Acquire(ctx context.Context) error {
	select {
	case l.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%w: %v", ErrLockTimeout, ctx.Err())
	}
}

func (l *timedLock) Release() {
	select {
	case <-l.sem:
	default:
	}
}

// WalletAccount holds a user's balance protected by its own context-aware lock.
type WalletAccount struct {
	UserID  string
	balance float64
	lock    *timedLock
}

// TransferReceipt summarizes a completed deadlock-free transfer.
type TransferReceipt struct {
	FromUserID      string    `json:"from_user_id"`
	ToUserID        string    `json:"to_user_id"`
	Amount          float64   `json:"amount"`
	FromNewBalance  float64   `json:"from_new_balance"`
	ToNewBalance    float64   `json:"to_new_balance"`
	LockOrder       []string  `json:"lock_acquisition_order"`
	ExecutedAt      time.Time `json:"executed_at"`
}

// WalletManager manages concurrent account transfers and multi-resource locks.
type WalletManager struct {
	mu          sync.RWMutex
	accounts    map[string]*WalletAccount
	lockTimeout time.Duration
}

// NewWalletManager creates a WalletManager seeded with initial balances from the catalog dataset.
func NewWalletManager(seeds []domain.WalletSeed) *WalletManager {
	wm := &WalletManager{
		accounts:    make(map[string]*WalletAccount, len(seeds)),
		lockTimeout: DefaultLockTimeout,
	}
	for _, s := range seeds {
		uid := strings.TrimSpace(s.UserID)
		if uid == "" {
			continue
		}
		wm.accounts[uid] = &WalletAccount{
			UserID:  uid,
			balance: roundCents(s.Balance),
			lock:    newTimedLock(),
		}
	}
	return wm
}

// Transfer atomically moves `amount` from `fromID` to `toID` using deterministic
// lexicographical lock ordering and a 500ms lock acquisition timeout.
func (wm *WalletManager) Transfer(ctx context.Context, fromID, toID string, amount float64) (*TransferReceipt, error) {
	fromID = strings.TrimSpace(fromID)
	toID = strings.TrimSpace(toID)

	if fromID == "" || toID == "" {
		return nil, ErrAccountNotFound
	}
	if fromID == toID {
		return nil, ErrSameAccount
	}
	if amount <= 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
		return nil, ErrInvalidAmount
	}
	amount = roundCents(amount)

	wm.mu.RLock()
	fromAcc, okFrom := wm.accounts[fromID]
	toAcc, okTo := wm.accounts[toID]
	wm.mu.RUnlock()

	if !okFrom {
		return nil, fmt.Errorf("%w: %s", ErrAccountNotFound, fromID)
	}
	if !okTo {
		return nil, fmt.Errorf("%w: %s", ErrAccountNotFound, toID)
	}

	// 1. DETERMINISTIC LOCK ORDERING:
	// Always lock the lexicographically smaller UserID first!
	firstAcc, secondAcc := fromAcc, toAcc
	if fromAcc.UserID > toAcc.UserID {
		firstAcc, secondAcc = toAcc, fromAcc
	}

	// 2. ENFORCE 500ms LOCK ACQUISITION TIMEOUT:
	lockCtx, cancel := context.WithTimeout(ctx, wm.lockTimeout)
	defer cancel()

	if err := firstAcc.lock.Acquire(lockCtx); err != nil {
		return nil, fmt.Errorf("failed locking first account %q: %w", firstAcc.UserID, err)
	}
	defer firstAcc.lock.Release()

	if err := secondAcc.lock.Acquire(lockCtx); err != nil {
		return nil, fmt.Errorf("failed locking second account %q: %w", secondAcc.UserID, err)
	}
	defer secondAcc.lock.Release()

	// 3. CRITICAL SECTION (Both locks held in deterministic order):
	if fromAcc.balance < amount {
		return nil, fmt.Errorf("%w: %s has %.2f, tried to transfer %.2f",
			ErrInsufficientBalance, fromID, fromAcc.balance, amount)
	}

	fromAcc.balance = roundCents(fromAcc.balance - amount)
	toAcc.balance = roundCents(toAcc.balance + amount)

	return &TransferReceipt{
		FromUserID:     fromID,
		ToUserID:       toID,
		Amount:         amount,
		FromNewBalance: fromAcc.balance,
		ToNewBalance:   toAcc.balance,
		LockOrder:      []string{firstAcc.UserID, secondAcc.UserID},
		ExecutedAt:     time.Now().UTC(),
	}, nil
}

// AcquireMultipleLocks demonstrates deterministic lock ordering across N arbitrary
// account/SKU identifiers (used when locking a basket of resources simultaneously).
func (wm *WalletManager) AcquireMultipleLocks(ctx context.Context, ids []string) (func(), []string, error) {
	uniqueMap := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		trimmed := strings.TrimSpace(id)
		if trimmed != "" {
			uniqueMap[trimmed] = struct{}{}
		}
	}

	sortedIDs := make([]string, 0, len(uniqueMap))
	for id := range uniqueMap {
		sortedIDs = append(sortedIDs, id)
	}
	sort.Strings(sortedIDs)

	lockCtx, cancel := context.WithTimeout(ctx, wm.lockTimeout)
	defer cancel()

	var acquired []*timedLock
	releaseAll := func() {
		// Release in reverse order (LIFO)
		for i := len(acquired) - 1; i >= 0; i-- {
			acquired[i].Release()
		}
	}

	wm.mu.RLock()
	defer wm.mu.RUnlock()

	for _, id := range sortedIDs {
		acc, ok := wm.accounts[id]
		if !ok {
			releaseAll()
			return nil, nil, fmt.Errorf("%w: %s", ErrAccountNotFound, id)
		}
		if err := acc.lock.Acquire(lockCtx); err != nil {
			releaseAll()
			return nil, nil, err
		}
		acquired = append(acquired, acc.lock)
	}

	return releaseAll, sortedIDs, nil
}

// SnapshotBalances safely reads all current wallet balances in deterministic order.
func (wm *WalletManager) SnapshotBalances(ctx context.Context) (map[string]float64, error) {
	wm.mu.RLock()
	ids := make([]string, 0, len(wm.accounts))
	for id := range wm.accounts {
		ids = append(ids, id)
	}
	wm.mu.RUnlock()

	release, _, err := wm.AcquireMultipleLocks(ctx, ids)
	if err != nil {
		return nil, err
	}
	defer release()

	snapshot := make(map[string]float64, len(ids))
	wm.mu.RLock()
	for _, id := range ids {
		snapshot[id] = wm.accounts[id].balance
	}
	wm.mu.RUnlock()
	return snapshot, nil
}

func roundCents(v float64) float64 {
	return math.Round(v*100) / 100
}

