package saga

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
)

var (
	ErrPaymentDeclined = errors.New("payment gateway declined card/wallet charge")
)

// SimulatedPaymentGateway implements domain.PaymentGateway with deterministic
// failure injection (`promo_code == "FAIL_PAYMENT"`) and an optional probabilistic
// decline rate for load testing Saga rollbacks.
type SimulatedPaymentGateway struct {
	declineRate float64
}

// NewSimulatedPaymentGateway creates a payment gateway simulator.
// If declineRate is between 0.0 and 1.0 (e.g. 0.10), that fraction of non-deterministic
// charges will randomly decline to exercise Saga compensations under load.
func NewSimulatedPaymentGateway(declineRate float64) *SimulatedPaymentGateway {
	return &SimulatedPaymentGateway{
		declineRate: declineRate,
	}
}

// Charge simulates authorizing and capturing `amount` for `orderID`.
func (pg *SimulatedPaymentGateway) Charge(ctx context.Context, orderID, userID string, amount float64, promoCode string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	// Deterministic failure trigger for integration & load testing
	if strings.EqualFold(strings.TrimSpace(promoCode), "FAIL_PAYMENT") {
		return "", fmt.Errorf("%w: deterministic decline triggered by promo_code=FAIL_PAYMENT for order %s", ErrPaymentDeclined, orderID)
	}

	// Optional probabilistic failure injection (e.g., 10% simulated gateway decline)
	if pg.declineRate > 0 && rand.Float64() < pg.declineRate {
		return "", fmt.Errorf("%w: simulated random bank decline (rate=%.0f%%) for order %s", ErrPaymentDeclined, pg.declineRate*100, orderID)
	}

	txID := fmt.Sprintf("pay-%s-%d", orderID, time.Now().UTC().UnixNano())
	return txID, nil
}

// Refund executes a compensating credit when a downstream step after payment fails.
func (pg *SimulatedPaymentGateway) Refund(ctx context.Context, _ string, _ string, _ float64) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

