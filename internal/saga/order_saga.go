package saga

import (
	"context"
	"fmt"
	"log/slog"

	"pulseorder/internal/domain"
)

// ============================================================================
// ARCHITECTURAL TEACHING: ORCHESTRATION SAGA & COMPENSATING TRANSACTIONS
// ============================================================================
// WHY NOT TWO-PHASE COMMIT (2PC / XA DISTRIBUTED LOCKS)?
// In a high-concurrency marketplace, processing an order spans three independent
// subsystems:
//   1. Inventory Service (Redis atomic SKU counters)
//   2. Payment Gateway (External card/wallet processor with 50ms–500ms latency)
//   3. Order Store & Event Bus (Redis + Kafka + Elasticsearch)
//
// If we used Two-Phase Commit (2PC), we would have to hold an exclusive lock
// on popular SKUs (like `TECH-MBP-14`) while waiting for an external payment
// gateway over the network. Under a 10x traffic spike, holding locks across
// network I/O starves all other buyers and causes cascading thread-pool exhaustion.
//
// HOW THE SAGA PATTERN SOLVES THIS:
// Instead of holding locks across steps, a Saga executes a sequence of local
// Forward Transactions ($T_1, T_2, T_3$), each committing immediately:
//   - $T_1$ (`ReserveInventoryStep`): Atomically decrements Redis stock now.
//   - $T_2$ (`ProcessPaymentStep`): Charges the customer's payment method.
//   - $T_3$ (`ConfirmOrderStep`): Marks order `COMPLETED` and emits event.
//
// If $T_2$ (`ProcessPaymentStep`) fails, $T_1$ has ALREADY committed in Redis!
// The Saga Orchestrator automatically walks BACKWARDS ($C_1$), executing
// `CompensateInventoryStep` (`INCRBY` via Lua script) to restore the exact SKU
// quantities back into Redis and marks the order `FAILED_COMPENSATED`.

// StepOutcomeObserver is an optional hook (used by Prometheus in Milestone 6)
// to record Saga completion vs. compensated rollback counts.
type StepOutcomeObserver func(outcome string)

type sagaStep struct {
	name           string
	forward        func(ctx context.Context, order *domain.Order) error
	compensateName string
	compensate     func(ctx context.Context, order *domain.Order) error
}

// OrderSagaOrchestrator coordinates the multi-step distributed Order workflow.
type OrderSagaOrchestrator struct {
	repo      domain.OrderRepository
	inventory domain.InventoryStore
	payment   domain.PaymentGateway
	publisher domain.EventPublisher
	logger    *slog.Logger
	observer  StepOutcomeObserver
}

// NewOrderSagaOrchestrator constructs a new Saga Orchestrator.
func NewOrderSagaOrchestrator(
	repo domain.OrderRepository,
	inventory domain.InventoryStore,
	payment domain.PaymentGateway,
	publisher domain.EventPublisher,
	logger *slog.Logger,
) *OrderSagaOrchestrator {
	return &OrderSagaOrchestrator{
		repo:      repo,
		inventory: inventory,
		payment:   payment,
		publisher: publisher,
		logger:    logger,
	}
}

// WithOutcomeObserver attaches a metrics callback (`completed` vs `compensated_rollback`).
func (s *OrderSagaOrchestrator) WithOutcomeObserver(obs StepOutcomeObserver) *OrderSagaOrchestrator {
	s.observer = obs
	return s
}

// Execute runs the forward Saga pipeline for `order` and triggers automatic
// reverse-order compensating transactions if any step fails.
func (s *OrderSagaOrchestrator) Execute(ctx context.Context, order *domain.Order) error {
	steps := []sagaStep{
		{
			name:           "ReserveInventoryStep",
			forward:        s.reserveInventoryForward,
			compensateName: "CompensateInventoryStep",
			compensate:     s.compensateInventoryBackward,
		},
		{
			name:           "ProcessPaymentStep",
			forward:        s.processPaymentForward,
			compensateName: "CompensatePaymentStep",
			compensate:     s.compensatePaymentBackward,
		},
		{
			name:           "ConfirmOrderStep",
			forward:        s.confirmOrderForward,
			compensateName: "",
			compensate:     nil,
		},
	}

	for idx, step := range steps {
		if err := step.forward(ctx, order); err != nil {
			s.logger.Warn("saga forward step failed, initiating compensating rollback",
				"order_id", order.ID(),
				"failed_step", step.name,
				"error", err,
			)
			order.RecordSagaStep(step.name, "FORWARD", "FAILED", err.Error())

			// Execute compensating transactions in strictly reverse order (idx-1 down to 0)
			compensatedAny := false
			for compIdx := idx - 1; compIdx >= 0; compIdx-- {
				prevStep := steps[compIdx]
				if prevStep.compensate == nil {
					continue
				}
				if compErr := prevStep.compensate(ctx, order); compErr != nil {
					s.logger.Error("CRITICAL: saga compensation step failed",
						"order_id", order.ID(),
						"compensate_step", prevStep.compensateName,
						"error", compErr,
					)
					order.RecordSagaStep(prevStep.compensateName, "COMPENSATE", "FAILED", compErr.Error())
				} else {
					compensatedAny = true
				}
			}

			if compensatedAny {
				_ = order.TransitionStatus(domain.StatusFailedCompensated)
				if s.observer != nil {
					s.observer("compensated_rollback")
				}
			} else {
				_ = order.TransitionStatus(domain.StatusFailed)
				if s.observer != nil {
					s.observer("failed_uncompensated")
				}
			}

			// Persist the rolled-back order state and Saga audit log
			if saveErr := s.repo.Save(ctx, order); saveErr != nil {
				s.logger.Error("failed saving compensated order state", "order_id", order.ID(), "error", saveErr)
			}

			// Emit OrderFinalizedEvent (`ORDER_FAILED_COMPENSATED`) to Kafka `orders.events`
			if s.publisher != nil {
				_ = s.publisher.PublishOrderEvent(ctx, "ORDER_FAILED_COMPENSATED", order)
			}

			return nil
		}
	}

	if s.observer != nil {
		s.observer("completed")
	}
	s.logger.Info("order saga completed successfully",
		"order_id", order.ID(),
		"status", order.Status(),
		"total_amount", order.TotalAmount(),
	)
	return nil
}

// --- Forward Steps ---

func (s *OrderSagaOrchestrator) reserveInventoryForward(ctx context.Context, order *domain.Order) error {
	if err := s.inventory.ReserveItems(ctx, order.Items()); err != nil {
		return err
	}
	_ = order.TransitionStatus(domain.StatusInventoryReserved)
	order.RecordSagaStep(
		"ReserveInventoryStep",
		"FORWARD",
		"SUCCESS",
		fmt.Sprintf("Atomically decremented Redis stock for %d SKU line item(s)", len(order.Items())),
	)
	return s.repo.Save(ctx, order)
}

func (s *OrderSagaOrchestrator) processPaymentForward(ctx context.Context, order *domain.Order) error {
	txID, err := s.payment.Charge(ctx, order.ID(), order.UserID(), order.TotalAmount(), order.PromoCode())
	if err != nil {
		return err
	}
	_ = order.TransitionStatus(domain.StatusPaymentProcessed)
	order.RecordSagaStep(
		"ProcessPaymentStep",
		"FORWARD",
		"SUCCESS",
		fmt.Sprintf("Charged $%.2f via PaymentGateway (tx_id=%s)", order.TotalAmount(), txID),
	)
	return s.repo.Save(ctx, order)
}

func (s *OrderSagaOrchestrator) confirmOrderForward(ctx context.Context, order *domain.Order) error {
	if err := order.TransitionStatus(domain.StatusCompleted); err != nil {
		return err
	}
	order.RecordSagaStep(
		"ConfirmOrderStep",
		"FORWARD",
		"SUCCESS",
		"Order finalized as COMPLETED and emitted to orders.events",
	)
	if err := s.repo.Save(ctx, order); err != nil {
		return err
	}
	if s.publisher != nil {
		if err := s.publisher.PublishOrderEvent(ctx, "ORDER_COMPLETED", order); err != nil {
			s.logger.Warn("failed emitting ORDER_COMPLETED event to kafka", "order_id", order.ID(), "error", err)
		}
	}
	return nil
}

// --- Compensating Steps (Backward Rollback) ---

func (s *OrderSagaOrchestrator) compensateInventoryBackward(ctx context.Context, order *domain.Order) error {
	if err := s.inventory.ReleaseItems(ctx, order.Items()); err != nil {
		return err
	}
	order.RecordSagaStep(
		"CompensateInventoryStep",
		"COMPENSATE",
		"SUCCESS",
		fmt.Sprintf("Rolled back and restored Redis stock for %d SKU line item(s)", len(order.Items())),
	)
	return nil
}

func (s *OrderSagaOrchestrator) compensatePaymentBackward(ctx context.Context, order *domain.Order) error {
	if err := s.payment.Refund(ctx, order.ID(), order.UserID(), order.TotalAmount()); err != nil {
		return err
	}
	order.RecordSagaStep(
		"CompensatePaymentStep",
		"COMPENSATE",
		"SUCCESS",
		fmt.Sprintf("Refunded $%.2f back to user %s", order.TotalAmount(), order.UserID()),
	)
	return nil
}

