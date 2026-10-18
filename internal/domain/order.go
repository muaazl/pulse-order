package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
)

// ============================================================================
// ARCHITECTURAL TEACHING: COMPOSITION OVER INHERITANCE (STRUCT EMBEDDING)
// ============================================================================
// Go intentionally omits class-based inheritance ("extends") to avoid the
// Fragile Base Class problem and deep virtual-dispatch hierarchies that hurt
// CPU cache locality under high concurrency.
//
// Instead, Go uses Composition via Struct Embedding. By embedding AuditMetadata
// directly into Order and Merchant, its fields (CreatedAt, UpdatedAt, Version)
// and methods (Touch) are automatically "promoted" to the outer struct while
// keeping the memory layout contiguous and flat.

// AuditMetadata tracks lifecycle timestamps and optimistic concurrency versions
// for domain entities. Embedded into both Order and Merchant.
type AuditMetadata struct {
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Version   int       `json:"version"`
}

// NewAuditMetadata initializes audit metadata at the current UTC timestamp.
func NewAuditMetadata() AuditMetadata {
	now := time.Now().UTC()
	return AuditMetadata{
		CreatedAt: now,
		UpdatedAt: now,
		Version:   1,
	}
}

// Touch increments the entity version and refreshes UpdatedAt on state mutation.
func (a *AuditMetadata) Touch() {
	a.UpdatedAt = time.Now().UTC()
	a.Version++
}

// OrderStatus represents a deterministic state in the Order Saga lifecycle.
type OrderStatus string

const (
	StatusAccepted          OrderStatus = "ACCEPTED"
	StatusInventoryReserved OrderStatus = "INVENTORY_RESERVED"
	StatusPaymentProcessed  OrderStatus = "PAYMENT_PROCESSED"
	StatusCompleted         OrderStatus = "COMPLETED"
	StatusFailedCompensated OrderStatus = "FAILED_COMPENSATED"
	StatusFailed            OrderStatus = "FAILED"
)

// OrderItem represents a single line item (SKU) within an order.
type OrderItem struct {
	SKU       string  `json:"sku"`
	Name      string  `json:"name"`
	Quantity  int     `json:"quantity"`
	UnitPrice float64 `json:"unit_price"`
}

// Subtotal calculates the line item total (Quantity * UnitPrice) rounded to 2 decimals.
func (i OrderItem) Subtotal() float64 {
	return roundCurrency(float64(i.Quantity) * i.UnitPrice)
}

// SagaLogEntry records an immutable audit trail entry for each forward or
// compensating step executed by the distributed Saga Orchestrator.
type SagaLogEntry struct {
	Step      string    `json:"step"`
	Action    string    `json:"action"` // "FORWARD" or "COMPENSATE"
	Status    string    `json:"status"` // "SUCCESS" or "FAILED"
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

// ============================================================================
// ARCHITECTURAL TEACHING: PACKAGE-LEVEL ENCAPSULATION (ACCESS MODIFIERS)
// ============================================================================
// In Go, visibility is controlled at the package boundary via identifier casing:
//   - Uppercase first letter = Exported (public outside `domain`)
//   - Lowercase first letter = Unexported (private to `domain`)
//
// WHY THIS MATTERS IN HIGH-CONCURRENCY SYSTEMS:
// If Order's fields (`totalAmount`, `status`, `items`, `surgeMultiplier`) were
// exported, any HTTP handler, middleware, or worker goroutine could arbitrarily
// mutate `order.TotalAmount = 0` or `order.Status = "COMPLETED"`, bypassing:
//   1. Pricing invariants (BaseAmount * SurgeMultiplier - Discount)
//   2. Valid Saga state machine transitions
//   3. Audit log & optimistic version increments (`AuditMetadata.Touch()`)
//   4. Slice ownership safety (external code mutating the `items` backing array)
//
// By keeping fields unexported, the `Order` can ONLY be constructed via the
// validated `OrderBuilder` and mutated through controlled domain methods.

// Order is the core aggregate root of PulseOrder.
type Order struct {
	AuditMetadata // Embedded struct (Composition over Inheritance)

	id              string
	userID          string
	zoneID          string
	items           []OrderItem
	promoCode       string
	baseAmount      float64
	discountAmount  float64
	surgeMultiplier float64
	totalAmount     float64
	isPriority      bool
	status          OrderStatus
	sagaLog         []SagaLogEntry
}

// --- Exported Read-Only Getters ---

func (o *Order) ID() string              { return o.id }
func (o *Order) UserID() string          { return o.userID }
func (o *Order) ZoneID() string          { return o.zoneID }
func (o *Order) PromoCode() string       { return o.promoCode }
func (o *Order) BaseAmount() float64     { return o.baseAmount }
func (o *Order) DiscountAmount() float64 { return o.discountAmount }
func (o *Order) SurgeMultiplier() float64 { return o.surgeMultiplier }
func (o *Order) TotalAmount() float64    { return o.totalAmount }
func (o *Order) IsPriority() bool        { return o.isPriority }
func (o *Order) Status() OrderStatus     { return o.status }

// Items returns a defensive copy of the order's line items so callers outside
// the domain package cannot mutate the internal slice backing array.
func (o *Order) Items() []OrderItem {
	copied := make([]OrderItem, len(o.items))
	copy(copied, o.items)
	return copied
}

// SagaLog returns a defensive copy of the Saga execution history.
func (o *Order) SagaLog() []SagaLogEntry {
	copied := make([]SagaLogEntry, len(o.sagaLog))
	copy(copied, o.sagaLog)
	return copied
}

// --- Controlled Domain State Transitions ---

// TransitionStatus enforces valid lifecycle state transitions and bumps the
// embedded AuditMetadata timestamp and version.
func (o *Order) TransitionStatus(newStatus OrderStatus) error {
	if newStatus == "" {
		return errors.New("order status cannot be empty")
	}
	if o.status == StatusCompleted && newStatus != StatusCompleted {
		return fmt.Errorf("cannot transition a COMPLETED order to %s", newStatus)
	}
	o.status = newStatus
	o.Touch()
	return nil
}

// RecordSagaStep appends an execution record to the order's Saga audit trail
// and updates the entity's audit metadata.
func (o *Order) RecordSagaStep(step, action, status, message string) {
	o.sagaLog = append(o.sagaLog, SagaLogEntry{
		Step:      step,
		Action:    action,
		Status:    status,
		Message:   message,
		Timestamp: time.Now().UTC(),
	})
	o.Touch()
}

// --- JSON Serialization Boundary ---
// Because Order fields are unexported to enforce encapsulation in Go code,
// we implement json.Marshaler and json.Unmarshaler using an internal DTO.
// This allows Redis, Kafka, Elasticsearch, and HTTP handlers to serialize
// Order aggregates cleanly without exposing mutable struct fields in Go.

type orderJSONSnapshot struct {
	ID              string         `json:"id"`
	UserID          string         `json:"user_id"`
	ZoneID          string         `json:"zone_id"`
	Items           []OrderItem    `json:"items"`
	PromoCode       string         `json:"promo_code,omitempty"`
	BaseAmount      float64        `json:"base_amount"`
	DiscountAmount  float64        `json:"discount_amount"`
	SurgeMultiplier float64        `json:"surge_multiplier"`
	TotalAmount     float64        `json:"total_amount"`
	IsPriority      bool           `json:"is_priority"`
	Status          OrderStatus    `json:"status"`
	SagaLog         []SagaLogEntry `json:"saga_log"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
	Version         int            `json:"version"`
}

// MarshalJSON implements json.Marshaler for safe wire serialization.
func (o Order) MarshalJSON() ([]byte, error) {
	return json.Marshal(orderJSONSnapshot{
		ID:              o.id,
		UserID:          o.userID,
		ZoneID:          o.zoneID,
		Items:           o.items,
		PromoCode:       o.promoCode,
		BaseAmount:      o.baseAmount,
		DiscountAmount:  o.discountAmount,
		SurgeMultiplier: o.surgeMultiplier,
		TotalAmount:     o.totalAmount,
		IsPriority:      o.isPriority,
		Status:          o.status,
		SagaLog:         o.sagaLog,
		CreatedAt:       o.CreatedAt,
		UpdatedAt:       o.UpdatedAt,
		Version:         o.Version,
	})
}

// UnmarshalJSON implements json.Unmarshaler to reconstruct an encapsulated Order
// from trusted persistence stores (Redis, Kafka, Elasticsearch).
func (o *Order) UnmarshalJSON(data []byte) error {
	var snap orderJSONSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	o.id = snap.ID
	o.userID = snap.UserID
	o.zoneID = snap.ZoneID
	o.items = snap.Items
	o.promoCode = snap.PromoCode
	o.baseAmount = snap.BaseAmount
	o.discountAmount = snap.DiscountAmount
	o.surgeMultiplier = snap.SurgeMultiplier
	o.totalAmount = snap.TotalAmount
	o.isPriority = snap.IsPriority
	o.status = snap.Status
	o.sagaLog = snap.SagaLog
	o.AuditMetadata = AuditMetadata{
		CreatedAt: snap.CreatedAt,
		UpdatedAt: snap.UpdatedAt,
		Version:   snap.Version,
	}
	return nil
}

// Merchant represents a marketplace vendor and demonstrates reusable composition
// by embedding the same AuditMetadata struct as Order.
type Merchant struct {
	AuditMetadata // Embedded struct (Composition over Inheritance)

	ID     string         `json:"id"`
	Name   string         `json:"name"`
	ZoneID string         `json:"zone_id"`
	Items  []MerchantItem `json:"items"`
}

// MerchantItem represents a catalog SKU and its initial stock level.
type MerchantItem struct {
	SKU          string  `json:"sku"`
	Name         string  `json:"name"`
	UnitPrice    float64 `json:"unit_price"`
	InitialStock int     `json:"initial_stock"`
}

// NewMerchant constructs a validated Merchant entity with initialized AuditMetadata.
func NewMerchant(id, name, zoneID string, items []MerchantItem) (*Merchant, error) {
	if id == "" || name == "" || zoneID == "" {
		return nil, errors.New("merchant id, name, and zone_id are required")
	}
	return &Merchant{
		AuditMetadata: NewAuditMetadata(),
		ID:            id,
		Name:          name,
		ZoneID:        zoneID,
		Items:         items,
	}, nil
}

// roundCurrency rounds a floating-point monetary amount to 2 decimal places.
func roundCurrency(val float64) float64 {
	return math.Round(val*100) / 100
}

