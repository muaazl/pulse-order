package domain_test

import (
	"encoding/json"
	"errors"
	"testing"

	"pulseorder/internal/domain"
)

func TestOrderBuilder_HappyPathAndPricing(t *testing.T) {
	items := []domain.OrderItem{
		{SKU: "SKU-LAPTOP-01", Name: "Pro Laptop 14", Quantity: 1, UnitPrice: 1000.00},
		{SKU: "SKU-MOUSE-02", Name: "Wireless Mouse", Quantity: 2, UnitPrice: 50.00},
	}

	// Base = 1000 + (2 * 50) = 1100.00
	// Surge = 1.3x -> Surged Gross = 1430.00
	// Promo "SAVE10" = 10% off -> Discount = 143.00
	// Total = 1430.00 - 143.00 = 1287.00
	order, err := domain.NewOrderBuilder().
		WithID("ord-custom-001").
		WithUser("user-42").
		WithZone("zone-downtown").
		WithItems(items).
		WithPromoCode("save10").
		WithPriority(true).
		WithSurgeMultiplier(1.3).
		Build()

	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if order.ID() != "ord-custom-001" {
		t.Errorf("expected ID ord-custom-001, got %s", order.ID())
	}
	if order.UserID() != "user-42" {
		t.Errorf("expected UserID user-42, got %s", order.UserID())
	}
	if order.ZoneID() != "zone-downtown" {
		t.Errorf("expected ZoneID zone-downtown, got %s", order.ZoneID())
	}
	if order.PromoCode() != "SAVE10" {
		t.Errorf("expected PromoCode SAVE10, got %s", order.PromoCode())
	}
	if !order.IsPriority() {
		t.Errorf("expected IsPriority to be true")
	}
	if order.BaseAmount() != 1100.00 {
		t.Errorf("expected BaseAmount 1100.00, got %.2f", order.BaseAmount())
	}
	if order.SurgeMultiplier() != 1.30 {
		t.Errorf("expected SurgeMultiplier 1.30, got %.2f", order.SurgeMultiplier())
	}
	if order.DiscountAmount() != 143.00 {
		t.Errorf("expected DiscountAmount 143.00, got %.2f", order.DiscountAmount())
	}
	if order.TotalAmount() != 1287.00 {
		t.Errorf("expected TotalAmount 1287.00, got %.2f", order.TotalAmount())
	}
	if order.Status() != domain.StatusAccepted {
		t.Errorf("expected initial status %s, got %s", domain.StatusAccepted, order.Status())
	}
	if order.Version != 1 {
		t.Errorf("expected initial embedded AuditMetadata.Version 1, got %d", order.Version)
	}
}

func TestOrderBuilder_ValidationEdgeCases(t *testing.T) {
	validItems := []domain.OrderItem{
		{SKU: "SKU-100", Name: "Headphones", Quantity: 1, UnitPrice: 99.99},
	}

	tests := []struct {
		name        string
		buildFn     func() (*domain.Order, error)
		expectedErr error
	}{
		{
			name: "missing user_id",
			buildFn: func() (*domain.Order, error) {
				return domain.NewOrderBuilder().
					WithZone("zone-downtown").
					WithItems(validItems).
					Build()
			},
			expectedErr: domain.ErrMissingUser,
		},
		{
			name: "missing zone_id",
			buildFn: func() (*domain.Order, error) {
				return domain.NewOrderBuilder().
					WithUser("user-1").
					WithItems(validItems).
					Build()
			},
			expectedErr: domain.ErrMissingZone,
		},
		{
			name: "unrecognized zone_id",
			buildFn: func() (*domain.Order, error) {
				return domain.NewOrderBuilder().
					WithUser("user-1").
					WithZone("zone-mars-colony").
					WithItems(validItems).
					Build()
			},
			expectedErr: domain.ErrInvalidZone,
		},
		{
			name: "empty items slice",
			buildFn: func() (*domain.Order, error) {
				return domain.NewOrderBuilder().
					WithUser("user-1").
					WithZone("zone-downtown").
					WithItems(nil).
					Build()
			},
			expectedErr: domain.ErrEmptyItems,
		},
		{
			name: "blank item SKU",
			buildFn: func() (*domain.Order, error) {
				return domain.NewOrderBuilder().
					WithUser("user-1").
					WithZone("zone-downtown").
					WithItems([]domain.OrderItem{{SKU: "   ", Quantity: 1, UnitPrice: 10.0}}).
					Build()
			},
			expectedErr: domain.ErrInvalidItemSKU,
		},
		{
			name: "zero item quantity",
			buildFn: func() (*domain.Order, error) {
				return domain.NewOrderBuilder().
					WithUser("user-1").
					WithZone("zone-downtown").
					WithItems([]domain.OrderItem{{SKU: "SKU-1", Quantity: 0, UnitPrice: 10.0}}).
					Build()
			},
			expectedErr: domain.ErrInvalidItemQuantity,
		},
		{
			name: "negative item price",
			buildFn: func() (*domain.Order, error) {
				return domain.NewOrderBuilder().
					WithUser("user-1").
					WithZone("zone-downtown").
					WithItems([]domain.OrderItem{{SKU: "SKU-1", Quantity: 1, UnitPrice: -5.00}}).
					Build()
			},
			expectedErr: domain.ErrInvalidItemPrice,
		},
		{
			name: "surge multiplier below 1.0",
			buildFn: func() (*domain.Order, error) {
				return domain.NewOrderBuilder().
					WithUser("user-1").
					WithZone("zone-downtown").
					WithItems(validItems).
					WithSurgeMultiplier(0.8).
					Build()
			},
			expectedErr: domain.ErrInvalidSurge,
		},
		{
			name: "surge multiplier above 5.0",
			buildFn: func() (*domain.Order, error) {
				return domain.NewOrderBuilder().
					WithUser("user-1").
					WithZone("zone-downtown").
					WithItems(validItems).
					WithSurgeMultiplier(6.5).
					Build()
			},
			expectedErr: domain.ErrInvalidSurge,
		},
		{
			name: "unsupported promo code",
			buildFn: func() (*domain.Order, error) {
				return domain.NewOrderBuilder().
					WithUser("user-1").
					WithZone("zone-downtown").
					WithItems(validItems).
					WithPromoCode("FREE_EVERYTHING_999").
					Build()
			},
			expectedErr: domain.ErrUnsupportedPromoCode,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			order, err := tc.buildFn()
			if err == nil {
				t.Fatalf("expected error %v, but got nil (order=%+v)", tc.expectedErr, order)
			}
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("expected error wrapping %v, got %v", tc.expectedErr, err)
			}
		})
	}
}

func TestOrder_EncapsulationAndDefensiveCopy(t *testing.T) {
	rawItems := []domain.OrderItem{
		{SKU: "SKU-A", Name: "Keyboard", Quantity: 2, UnitPrice: 75.00},
	}

	order, err := domain.NewOrderBuilder().
		WithUser("user-encap").
		WithZone("zone-midtown").
		WithItems(rawItems).
		Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Mutate caller's input slice after Build()
	rawItems[0].UnitPrice = 0.01
	if order.Items()[0].UnitPrice != 75.00 {
		t.Fatalf("encapsulation breach: mutating input slice affected Order internal items")
	}

	// Mutate slice returned by getter order.Items()
	fetchedItems := order.Items()
	fetchedItems[0].Quantity = 9999
	if order.Items()[0].Quantity != 2 {
		t.Fatalf("encapsulation breach: mutating getter slice affected Order internal items")
	}
}

func TestOrder_CompositionAndJSONRoundTrip(t *testing.T) {
	order, err := domain.NewOrderBuilder().
		WithUser("user-99").
		WithZone("zone-uptown").
		WithItems([]domain.OrderItem{{SKU: "SKU-9", Name: "Monitor", Quantity: 1, UnitPrice: 300.00}}).
		WithPromoCode("FAIL_PAYMENT").
		WithSurgeMultiplier(1.6).
		Build()
	if err != nil {
		t.Fatalf("unexpected build error: %v", err)
	}

	// Transition status & record saga step -> verify embedded AuditMetadata.Version increments
	if err := order.TransitionStatus(domain.StatusInventoryReserved); err != nil {
		t.Fatalf("unexpected status transition error: %v", err)
	}
	order.RecordSagaStep("ReserveInventory", "FORWARD", "SUCCESS", "Reserved 1x SKU-9 in Redis")

	if order.Version != 3 {
		t.Errorf("expected embedded Version to be 3 after 2 mutations, got %d", order.Version)
	}

	// Verify JSON Marshal & Unmarshal preserves unexported fields
	payload, err := json.Marshal(order)
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	var restored domain.Order
	if err := json.Unmarshal(payload, &restored); err != nil {
		t.Fatalf("UnmarshalJSON failed: %v", err)
	}

	if restored.ID() != order.ID() || restored.TotalAmount() != 480.00 || restored.Status() != domain.StatusInventoryReserved {
		t.Fatalf("restored order mismatch: got ID=%s Total=%.2f Status=%s",
			restored.ID(), restored.TotalAmount(), restored.Status())
	}
	if len(restored.SagaLog()) != 2 {
		t.Fatalf("expected 2 saga log entries after round-trip, got %d", len(restored.SagaLog()))
	}

	// Verify Merchant also embeds AuditMetadata via composition
	merchant, err := domain.NewMerchant("merch-1", "TechHub", "zone-uptown", []domain.MerchantItem{
		{SKU: "SKU-9", Name: "Monitor", UnitPrice: 300.00, InitialStock: 50},
	})
	if err != nil {
		t.Fatalf("unexpected merchant creation error: %v", err)
	}
	if merchant.Version != 1 || merchant.CreatedAt.IsZero() {
		t.Errorf("expected Merchant to promote embedded AuditMetadata fields")
	}
}
