package messaging_test

import (
	"context"
	"testing"

	"pulseorder/internal/messaging"
)

func TestNextPriorityMessage_DrainsHighPriorityFirst(t *testing.T) {
	ctx := context.Background()
	highCh := make(chan string, 5)
	stdCh := make(chan string, 5)

	// Enqueue 3 standard orders and 3 VIP priority orders simultaneously
	stdCh <- "std-1"
	stdCh <- "std-2"
	stdCh <- "std-3"

	highCh <- "vip-1"
	highCh <- "vip-2"
	highCh <- "vip-3"

	var drained []string
	for i := 0; i < 6; i++ {
		msg, ok := messaging.NextPriorityMessage(ctx, highCh, stdCh)
		if !ok {
			t.Fatalf("expected message at index %d", i)
		}
		drained = append(drained, msg)
	}

	expected := []string{"vip-1", "vip-2", "vip-3", "std-1", "std-2", "std-3"}
	for i := range expected {
		if drained[i] != expected[i] {
			t.Fatalf("priority drain order violated at index %d: expected %v, got %v", i, expected, drained)
		}
	}
}

