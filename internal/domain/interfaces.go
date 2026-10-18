package domain

import "context"

// ============================================================================
// ARCHITECTURAL TEACHING: SMALL, IMPLICIT INTERFACES (COMPOSITION)
// ============================================================================
// Unlike Java or C# where classes must explicitly declare `implements OrderRepository`,
// Go interfaces are satisfied IMPLICITLY (structural typing).
//
// Furthermore, in idiomatic Go architecture:
//   1. Interfaces belong in the domain/consumer package, NOT the infrastructure package.
//   2. Interfaces are kept small (1–3 focused methods) following the Interface
//      Segregation Principle (ISP).
//   3. High-concurrency unit tests can substitute Redis, Kafka, or Payment gateways
//      with zero-allocation in-memory fakes without importing heavy mocking frameworks.

// OrderRepository defines persistence operations for the Order aggregate.
type OrderRepository interface {
	Save(ctx context.Context, order *Order) error
	GetByID(ctx context.Context, orderID string) (*Order, error)
}

// EventPublisher decouples domain state transitions from the underlying message
// broker (Apache Kafka). Partition keys ensure strict per-entity ordering.
type EventPublisher interface {
	PublishOrderAccepted(ctx context.Context, order *Order) error
	PublishOrderEvent(ctx context.Context, eventType string, order *Order) error
}

// PaymentGateway abstracts external card/wallet charging and refunds inside
// the Saga Orchestrator.
type PaymentGateway interface {
	Charge(ctx context.Context, orderID, userID string, amount float64, promoCode string) (txID string, err error)
	Refund(ctx context.Context, orderID, userID string, amount float64) error
}

// InventoryStore abstracts atomic SKU stock reservation and compensation in Redis.
type InventoryStore interface {
	ReserveItems(ctx context.Context, items []OrderItem) error
	ReleaseItems(ctx context.Context, items []OrderItem) error
}
