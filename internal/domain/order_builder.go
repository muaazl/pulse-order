package domain

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ============================================================================
// ARCHITECTURAL TEACHING: THE BUILDER PATTERN IN HIGH-CONCURRENCY GO
// ============================================================================
// WHY THE BUILDER PATTERN?
// 1. No Function Overloading or Default Arguments in Go:
//    Constructing a rich domain object with optional parameters (PromoCode,
//    IsPriority, SurgeMultiplier, explicit ID) using positional arguments leads
//    to brittle call sites (`NewOrder(u, z, items, "", false, 1.0)`).
//
// 2. "Parse, Don't Validate" — Atomic Invariant Enforcement at Construction:
//    In a distributed event-driven pipeline (HTTP -> Redis -> Kafka -> Saga -> ES),
//    allowing a partially initialized `Order` struct into memory risks poisoning
//    downstream Kafka consumers with NaN prices, negative quantities, or empty
//    partition keys (`userID`, `zoneID`).
//    Because `Order` fields are unexported and `OrderBuilder.Build()` is the ONLY
//    factory entry point, any `*Order` pointer in the system is guaranteed to
//    have passed all business invariants and pricing calculations.
//
// 3. Trade-offs in High-Concurrency Systems:
//    - Trade-off: Allocating a separate builder struct introduces a tiny struct
//      overhead per HTTP request. However, Go's escape analysis often keeps
//      short-lived builders on the goroutine stack or collects them in the young
//      generation with negligible GC pressure.
//    - Concurrency Rule: `OrderBuilder` itself is NOT shared across goroutines;
//      it is used within a single request goroutine to assemble state and then
//      discarded once `.Build()` emits the encapsulated `*Order`.

// ValidZones defines the supported marketplace delivery zones.
var ValidZones = map[string]bool{
	"zone-downtown": true,
	"zone-midtown":  true,
	"zone-uptown":   true,
	"zone-westside": true,
	"zone-eastside": true,
	"zone-airport":  true,
}

// Validation errors returned by OrderBuilder.Build() (mapped to HTTP 400 Bad Request).
var (
	ErrMissingUser           = errors.New("user_id is required")
	ErrMissingZone           = errors.New("zone_id is required")
	ErrInvalidZone           = errors.New("zone_id is not a recognized marketplace zone")
	ErrEmptyItems            = errors.New("order must contain at least one item")
	ErrInvalidItemSKU        = errors.New("every order item must have a non-empty SKU")
	ErrInvalidItemQuantity   = errors.New("every order item quantity must be greater than zero")
	ErrInvalidItemPrice      = errors.New("every order item unit_price must be strictly positive")
	ErrInvalidSurge          = errors.New("surge_multiplier must be between 1.0 and 5.0")
	ErrUnsupportedPromoCode  = errors.New("unsupported promo_code")
)

// OrderBuilder provides a fluent interface for step-by-step construction and
// validation of an encapsulated Order aggregate.
type OrderBuilder struct {
	id              string
	userID          string
	zoneID          string
	items           []OrderItem
	promoCode       string
	isPriority      bool
	surgeMultiplier float64
}

// NewOrderBuilder initializes an OrderBuilder with safe defaults
// (1.0x neutral surge multiplier, standard priority).
func NewOrderBuilder() *OrderBuilder {
	return &OrderBuilder{
		surgeMultiplier: 1.0,
		isPriority:      false,
	}
}

// WithID sets an explicit order identifier (optional; auto-generated if omitted).
func (b *OrderBuilder) WithID(id string) *OrderBuilder {
	b.id = strings.TrimSpace(id)
	return b
}

// WithUser sets the customer user ID (used as the Kafka partition key for strict ordering).
func (b *OrderBuilder) WithUser(userID string) *OrderBuilder {
	b.userID = strings.TrimSpace(userID)
	return b
}

// WithZone sets the geographical zone ID (used for Flink surge pricing lookup).
func (b *OrderBuilder) WithZone(zoneID string) *OrderBuilder {
	b.zoneID = strings.ToLower(strings.TrimSpace(zoneID))
	return b
}

// WithItems sets the line items on the builder using a defensive slice copy.
func (b *OrderBuilder) WithItems(items []OrderItem) *OrderBuilder {
	if items == nil {
		b.items = nil
		return b
	}
	copied := make([]OrderItem, len(items))
	copy(copied, items)
	b.items = copied
	return b
}

// WithPromoCode applies an optional promotional code (e.g., "SAVE10", "VIP20",
// or "FAIL_PAYMENT" for Saga compensation testing).
func (b *OrderBuilder) WithPromoCode(code string) *OrderBuilder {
	b.promoCode = strings.ToUpper(strings.TrimSpace(code))
	return b
}

// WithPriority flags the order for routing to the high-priority Kafka topic
// (`orders.priority.high`).
func (b *OrderBuilder) WithPriority(isPriority bool) *OrderBuilder {
	b.isPriority = isPriority
	return b
}

// WithSurgeMultiplier applies the real-time zone surge multiplier fetched from Redis.
func (b *OrderBuilder) WithSurgeMultiplier(multiplier float64) *OrderBuilder {
	b.surgeMultiplier = multiplier
	return b
}

// Build validates all domain invariants, computes the base amount, surge pricing,
// and promotional discounts, and returns an encapsulated *Order.
func (b *OrderBuilder) Build() (*Order, error) {
	// 1. Validate User ID (required for Kafka partition key & rate limiting)
	if b.userID == "" {
		return nil, ErrMissingUser
	}

	// 2. Validate Zone ID (must be non-empty and in our supported zone registry)
	if b.zoneID == "" {
		return nil, ErrMissingZone
	}
	if !ValidZones[b.zoneID] {
		return nil, fmt.Errorf("%w: %q", ErrInvalidZone, b.zoneID)
	}

	// 3. Validate Line Items & Compute Base Amount
	if len(b.items) == 0 {
		return nil, ErrEmptyItems
	}

	var baseAmount float64
	normalizedItems := make([]OrderItem, len(b.items))
	for i, item := range b.items {
		sku := strings.TrimSpace(item.SKU)
		if sku == "" {
			return nil, fmt.Errorf("%w at index %d", ErrInvalidItemSKU, i)
		}
		if item.Quantity <= 0 {
			return nil, fmt.Errorf("%w for SKU %q: got %d", ErrInvalidItemQuantity, sku, item.Quantity)
		}

		unitPrice := item.UnitPrice
		name := strings.TrimSpace(item.Name)
		if catalogItem, found := DefaultCatalog().LookupSKU(sku); found {
			sku = catalogItem.SKU
			if unitPrice == 0 {
				unitPrice = catalogItem.UnitPrice
			}
			if name == "" {
				name = catalogItem.Name
			}
		}

		if unitPrice <= 0 || isNaNOrInf(unitPrice) {
			return nil, fmt.Errorf("%w for SKU %q: got %.2f", ErrInvalidItemPrice, sku, unitPrice)
		}

		if name == "" {
			name = sku
		}

		normalizedItems[i] = OrderItem{
			SKU:       sku,
			Name:      name,
			Quantity:  item.Quantity,
			UnitPrice: roundCurrency(unitPrice),
		}
		baseAmount += normalizedItems[i].Subtotal()
	}
	baseAmount = roundCurrency(baseAmount)

	// 4. Validate Surge Multiplier
	if isNaNOrInf(b.surgeMultiplier) || b.surgeMultiplier < 1.0 || b.surgeMultiplier > 5.0 {
		return nil, fmt.Errorf("%w: got %.2f", ErrInvalidSurge, b.surgeMultiplier)
	}
	surgeMultiplier := roundCurrency(b.surgeMultiplier)

	// 5. Validate & Compute Promotional Discount
	discountRate, err := resolvePromoDiscountRate(b.promoCode)
	if err != nil {
		return nil, err
	}

	surgedGross := roundCurrency(baseAmount * surgeMultiplier)
	discountAmount := roundCurrency(surgedGross * discountRate)
	totalAmount := roundCurrency(surgedGross - discountAmount)
	if totalAmount < 0.01 {
		totalAmount = 0.01
	}

	// 6. Generate Order ID if not explicitly provided
	orderID := b.id
	if orderID == "" {
		orderID = generateOrderID()
	}

	order := &Order{
		AuditMetadata:   NewAuditMetadata(),
		id:              orderID,
		userID:          b.userID,
		zoneID:          b.zoneID,
		items:           normalizedItems,
		promoCode:       b.promoCode,
		baseAmount:      baseAmount,
		discountAmount:  discountAmount,
		surgeMultiplier: surgeMultiplier,
		totalAmount:     totalAmount,
		isPriority:      b.isPriority,
		status:          StatusAccepted,
		sagaLog: []SagaLogEntry{
			{
				Step:      "OrderIngested",
				Action:    "FORWARD",
				Status:    "SUCCESS",
				Message:   "Order validated via OrderBuilder and accepted for async Saga processing",
				Timestamp: time.Now().UTC(),
			},
		},
	}

	return order, nil
}

// resolvePromoDiscountRate maps known promo codes to percentage discounts.
// Note: "FAIL_PAYMENT" is a valid synthetic promo code used in Milestone 3 & 6
// to deterministically trigger a payment failure and verify Saga stock rollback.
func resolvePromoDiscountRate(code string) (float64, error) {
	switch code {
	case "":
		return 0.0, nil
	case "SAVE10":
		return 0.10, nil
	case "VIP20":
		return 0.20, nil
	case "FAIL_PAYMENT":
		return 0.0, nil
	default:
		return 0.0, fmt.Errorf("%w: %q (supported: SAVE10, VIP20, FAIL_PAYMENT)", ErrUnsupportedPromoCode, code)
	}
}

func generateOrderID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return fmt.Sprintf("ord-%d-%s", time.Now().UTC().UnixMilli(), hex.EncodeToString(b))
}

func isNaNOrInf(v float64) bool {
	return v != v || v > 1e15 || v < -1e15
}

