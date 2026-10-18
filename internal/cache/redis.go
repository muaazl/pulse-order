package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"pulseorder/internal/domain"
)

var (
	ErrOrderNotFound     = errors.New("order not found in redis store")
	ErrInsufficientStock = errors.New("insufficient inventory stock for one or more SKUs")
)

// ============================================================================
// ARCHITECTURAL TEACHING: ATOMIC MULTI-SKU STOCK RESERVATION VIA REDIS LUA
// ============================================================================
// WHY NOT USE `GET` FOLLOWED BY `DECRBY` IN GO?
// Under high concurrency (e.g., 500 users buying the last 5 MacBook Pros at the
// same millisecond), doing `GET stock:sku:TECH-MBP-14` in Go, checking `if stock >= 1`,
// and then sending `DECRBY` introduces a classic Time-Of-Check to Time-Of-Use
// (TOCTOU) race condition, resulting in oversold negative inventory.
//
// Furthermore, an order may contain multiple SKUs. If SKU #1 succeeds and SKU #2
// is out of stock, we must not leave SKU #1 partially decremented.
//
// Because Redis executes Lua scripts single-threaded and atomically, this script:
//   Phase 1: Checks that EVERY SKU in the order has `current_stock >= requested_qty`.
//   Phase 2: Only if ALL pass, decrements every SKU in the same atomic execution.
var reserveInventoryLua = redis.NewScript(`
-- KEYS: ["stock:sku:SKU_1", "stock:sku:SKU_2", ...]
-- ARGV: [qty_1, qty_2, ...]
for i = 1, #KEYS do
    local current = tonumber(redis.call("GET", KEYS[i]) or "0")
    local requested = tonumber(ARGV[i])
    if current < requested then
        return {0, KEYS[i], current, requested}
    end
end

for i = 1, #KEYS do
    redis.call("DECRBY", KEYS[i], tonumber(ARGV[i]))
end

return {1, "OK", 0, 0}
`)

// releaseInventoryLua atomically restores reserved inventory during a Saga
// compensating rollback (`CompensateInventoryStep`).
var releaseInventoryLua = redis.NewScript(`
for i = 1, #KEYS do
    redis.call("INCRBY", KEYS[i], tonumber(ARGV[i]))
end
return 1
`)

// RedisStore manages all speed-layer state in Redis:
// - Order persistence (`order:{id}`)
// - Atomic SKU stock counters (`stock:sku:{sku}`)
// - Merchant registry (`merchant:{id}`)
type RedisStore struct {
	client *redis.Client
}

// NewRedisStore initializes a pooled Redis client.
func NewRedisStore(addr, password string, db int) *RedisStore {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     password,
		DB:           db,
		PoolSize:     64,
		MinIdleConns: 10,
		DialTimeout:  3 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})
	return &RedisStore{client: client}
}

// NewRedisStoreFromClient wraps an existing *redis.Client (useful for miniredis unit tests).
func NewRedisStoreFromClient(client *redis.Client) *RedisStore {
	return &RedisStore{client: client}
}

// Client returns the underlying *redis.Client for middleware (rate limiter, idempotency, surge cache).
func (s *RedisStore) Client() *redis.Client {
	return s.client
}

// Ping verifies connectivity to Redis.
func (s *RedisStore) Ping(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

// Close cleanly shuts down the Redis connection pool.
func (s *RedisStore) Close() error {
	return s.client.Close()
}

// --- domain.OrderRepository Implementation ---

func orderKey(orderID string) string {
	return "order:" + strings.TrimSpace(orderID)
}

func stockKey(sku string) string {
	return "stock:sku:" + strings.ToUpper(strings.TrimSpace(sku))
}

func merchantKey(merchantID string) string {
	return "merchant:" + strings.TrimSpace(merchantID)
}

// Save persists an Order aggregate as JSON in Redis with a 24-hour retention TTL.
func (s *RedisStore) Save(ctx context.Context, order *domain.Order) error {
	data, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("marshal order %s: %w", order.ID(), err)
	}
	return s.client.Set(ctx, orderKey(order.ID()), data, 24*time.Hour).Err()
}

// GetByID retrieves and reconstructs an encapsulated Order aggregate from Redis.
func (s *RedisStore) GetByID(ctx context.Context, orderID string) (*domain.Order, error) {
	data, err := s.client.Get(ctx, orderKey(orderID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrOrderNotFound
		}
		return nil, err
	}
	var order domain.Order
	if err := json.Unmarshal(data, &order); err != nil {
		return nil, fmt.Errorf("unmarshal order %s: %w", orderID, err)
	}
	return &order, nil
}

// --- domain.InventoryStore Implementation (Saga Forward & Compensating Steps) ---

// ReserveItems atomically verifies and decrements stock for all line items in an order.
func (s *RedisStore) ReserveItems(ctx context.Context, items []domain.OrderItem) error {
	if len(items) == 0 {
		return nil
	}
	keys := make([]string, len(items))
	args := make([]any, len(items))
	for i, item := range items {
		keys[i] = stockKey(item.SKU)
		args[i] = item.Quantity
	}

	res, err := reserveInventoryLua.Run(ctx, s.client, keys, args...).Slice()
	if err != nil {
		return fmt.Errorf("execute reserveInventoryLua: %w", err)
	}

	status, _ := res[0].(int64)
	if status == 0 {
		failedKey, _ := res[1].(string)
		available, _ := res[2].(int64)
		requested, _ := res[3].(int64)
		sku := strings.TrimPrefix(failedKey, "stock:sku:")
		return fmt.Errorf("%w: SKU %s has %d available, requested %d",
			ErrInsufficientStock, sku, available, requested)
	}
	return nil
}

// ReleaseItems atomically restores stock for all line items when a Saga rolls back.
func (s *RedisStore) ReleaseItems(ctx context.Context, items []domain.OrderItem) error {
	if len(items) == 0 {
		return nil
	}
	keys := make([]string, len(items))
	args := make([]any, len(items))
	for i, item := range items {
		keys[i] = stockKey(item.SKU)
		args[i] = item.Quantity
	}
	return releaseInventoryLua.Run(ctx, s.client, keys, args...).Err()
}

// --- Catalog Seeding & Stock Inspection ---

// SeedCatalog populates Redis with all merchants, SKU initial stocks, and neutral
// 1.0x zone surge multipliers from the curated CatalogDataset.
func (s *RedisStore) SeedCatalog(ctx context.Context, catalog *domain.CatalogDataset) error {
	pipe := s.client.Pipeline()

	for _, merchant := range catalog.Merchants {
		data, err := json.Marshal(merchant)
		if err != nil {
			return err
		}
		pipe.Set(ctx, merchantKey(merchant.ID), data, 0)
		for _, item := range merchant.Items {
			pipe.Set(ctx, stockKey(item.SKU), item.InitialStock, 0)
		}
	}

	for zone := range domain.ValidZones {
		pipe.Set(ctx, "surge:zone:"+zone, "1.00", 5*time.Minute)
	}

	_, err := pipe.Exec(ctx)
	return err
}

// SaveMerchant persists a newly registered Merchant and initializes its SKU stocks in Redis.
func (s *RedisStore) SaveMerchant(ctx context.Context, merchant *domain.Merchant) error {
	data, err := json.Marshal(merchant)
	if err != nil {
		return err
	}
	pipe := s.client.Pipeline()
	pipe.Set(ctx, merchantKey(merchant.ID), data, 0)
	for _, item := range merchant.Items {
		pipe.Set(ctx, stockKey(item.SKU), item.InitialStock, 0)
	}
	_, err = pipe.Exec(ctx)
	return err
}

// GetSKUStockMap returns the live Redis stock count for each provided SKU.
func (s *RedisStore) GetSKUStockMap(ctx context.Context, skus []string) (map[string]int, error) {
	result := make(map[string]int, len(skus))
	if len(skus) == 0 {
		return result, nil
	}

	keys := make([]string, len(skus))
	for i, sku := range skus {
		keys[i] = stockKey(sku)
	}

	vals, err := s.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	for i, raw := range vals {
		sku := strings.ToUpper(strings.TrimSpace(skus[i]))
		if raw == nil {
			result[sku] = 0
			continue
		}
		if strVal, ok := raw.(string); ok {
			parsed, _ := strconv.Atoi(strVal)
			result[sku] = parsed
		}
	}
	return result, nil
}

// IncrementSagaOutcome increments a Redis counter for the given Saga outcome
// (`completed` vs `compensated_rollback`) so `cmd/worker` and `cmd/api` can share
// cluster-wide Saga telemetry.
func (s *RedisStore) IncrementSagaOutcome(ctx context.Context, outcome string) {
	_ = s.client.Incr(ctx, "metrics:saga:"+strings.TrimSpace(outcome)).Err()
}


