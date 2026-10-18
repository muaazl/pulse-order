package saga_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"pulseorder/internal/cache"
	"pulseorder/internal/domain"
	"pulseorder/internal/saga"
)

type spyEventPublisher struct {
	events []string
}

func (s *spyEventPublisher) PublishOrderAccepted(_ context.Context, _ *domain.Order) error {
	s.events = append(s.events, "ORDER_ACCEPTED")
	return nil
}

func (s *spyEventPublisher) PublishOrderEvent(_ context.Context, eventType string, _ *domain.Order) error {
	s.events = append(s.events, eventType)
	return nil
}

func setupSagaTestEnv(t *testing.T) (*cache.RedisStore, *spyEventPublisher, *saga.OrderSagaOrchestrator) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis failed: %v", err)
	}
	t.Cleanup(func() { mr.Close() })

	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := cache.NewRedisStoreFromClient(rc)
	if err := store.SeedCatalog(context.Background(), domain.DefaultCatalog()); err != nil {
		t.Fatalf("SeedCatalog failed: %v", err)
	}

	pub := &spyEventPublisher{}
	pg := saga.NewSimulatedPaymentGateway(0.0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	orch := saga.NewOrderSagaOrchestrator(store, store, pg, pub, logger)

	return store, pub, orch
}

func TestOrderSaga_HappyPath_ReservesChargesConfirms(t *testing.T) {
	store, pub, orch := setupSagaTestEnv(t)
	ctx := context.Background()

	// TECH-MBP-14 starts with initial_stock = 50
	order, err := domain.NewOrderBuilder().
		WithUser("user-alice").
		WithZone("zone-downtown").
		WithItems([]domain.OrderItem{{SKU: "TECH-MBP-14", Quantity: 2}}).
		Build()
	if err != nil {
		t.Fatalf("OrderBuilder failed: %v", err)
	}

	if err := orch.Execute(ctx, order); err != nil {
		t.Fatalf("Saga Execute failed: %v", err)
	}

	// Verify final order status in Redis is COMPLETED
	saved, err := store.GetByID(ctx, order.ID())
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}
	if saved.Status() != domain.StatusCompleted {
		t.Fatalf("expected status COMPLETED, got %s", saved.Status())
	}

	// Verify Redis stock decremented from 50 -> 48
	stocks, _ := store.GetSKUStockMap(ctx, []string{"TECH-MBP-14"})
	if stocks["TECH-MBP-14"] != 48 {
		t.Errorf("expected remaining stock 48, got %d", stocks["TECH-MBP-14"])
	}

	// Verify ORDER_COMPLETED event emitted
	if len(pub.events) != 1 || pub.events[0] != "ORDER_COMPLETED" {
		t.Errorf("expected [ORDER_COMPLETED] event, got %v", pub.events)
	}
}

func TestOrderSaga_PaymentFailure_AutomaticCompensatingInventoryRollback(t *testing.T) {
	store, pub, orch := setupSagaTestEnv(t)
	ctx := context.Background()

	var recordedOutcome string
	orch.WithOutcomeObserver(func(outcome string) {
		recordedOutcome = outcome
	})

	// Order 5x TECH-MBP-14 with promo_code="FAIL_PAYMENT"
	order, err := domain.NewOrderBuilder().
		WithUser("user-bob").
		WithZone("zone-downtown").
		WithItems([]domain.OrderItem{{SKU: "TECH-MBP-14", Quantity: 5}}).
		WithPromoCode("FAIL_PAYMENT").
		Build()
	if err != nil {
		t.Fatalf("OrderBuilder failed: %v", err)
	}

	if err := orch.Execute(ctx, order); err != nil {
		t.Fatalf("Saga Execute returned unexpected error: %v", err)
	}

	// 1. Verify order transitioned to FAILED_COMPENSATED
	saved, err := store.GetByID(ctx, order.ID())
	if err != nil {
		t.Fatalf("GetByID failed: %v", err)
	}
	if saved.Status() != domain.StatusFailedCompensated {
		t.Fatalf("expected status FAILED_COMPENSATED, got %s", saved.Status())
	}
	if recordedOutcome != "compensated_rollback" {
		t.Errorf("expected observer outcome 'compensated_rollback', got %q", recordedOutcome)
	}

	// 2. Verify CompensateInventoryStep restored TECH-MBP-14 stock back to 50!
	stocks, _ := store.GetSKUStockMap(ctx, []string{"TECH-MBP-14"})
	if stocks["TECH-MBP-14"] != 50 {
		t.Fatalf("compensating stock rollback failed! expected stock 50, got %d", stocks["TECH-MBP-14"])
	}

	// 3. Verify full Saga execution audit log on the Order aggregate
	logs := saved.SagaLog()
	if len(logs) != 4 {
		t.Fatalf("expected 4 SagaLog entries (Ingested, ReserveOK, PaymentFail, CompensateOK), got %d: %+v", len(logs), logs)
	}
	if logs[1].Step != "ReserveInventoryStep" || logs[1].Status != "SUCCESS" {
		t.Errorf("unexpected step 1 log: %+v", logs[1])
	}
	if logs[2].Step != "ProcessPaymentStep" || logs[2].Status != "FAILED" {
		t.Errorf("unexpected step 2 log: %+v", logs[2])
	}
	if logs[3].Step != "CompensateInventoryStep" || logs[3].Action != "COMPENSATE" || logs[3].Status != "SUCCESS" {
		t.Errorf("unexpected step 3 compensation log: %+v", logs[3])
	}

	// 4. Verify ORDER_FAILED_COMPENSATED event emitted
	if len(pub.events) != 1 || pub.events[0] != "ORDER_FAILED_COMPENSATED" {
		t.Errorf("expected [ORDER_FAILED_COMPENSATED] event, got %v", pub.events)
	}
}

