package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"pulseorder/internal/domain"
)

// ============================================================================
// ARCHITECTURAL TEACHING: WEIGHTED PRIORITY CONSUMER LOOP IN GO
// ============================================================================
// In Go, when a single `select` statement has multiple ready channels:
//     select {
//     case msg := <-highPriorityCh:
//     case msg := <-standardCh:
//     }
// the Go runtime chooses between ready cases using a UNIFORM PSEUDO-RANDOM
// distribution (50% / 50%). That means under heavy load, standard orders would
// still steal 50% of worker capacity from VIP/Priority orders!
//
// To enforce strict priority draining without starvation, we use a two-tier
// non-blocking `select` with `default`:
//   1. First `select` checks `highPriorityCh` with a `default` branch.
//      If ANY message is waiting in `orders.priority.high`, it is consumed
//      immediately.
//   2. Only when `highPriorityCh` is currently empty does execution enter the
//      second blocking `select` that listens to both `highPriorityCh` and
//      `standardCh`.

// PriorityOrderMessage wraps a deserialized Order and its source Kafka topic.
type PriorityOrderMessage struct {
	Topic string
	Order *domain.Order
	ack   func(ctx context.Context) error
}

// Ack commits the Kafka offset after the Saga Orchestrator has finalized the order.
func (m *PriorityOrderMessage) Ack(ctx context.Context) error {
	if m.ack == nil {
		return nil
	}
	return m.ack(ctx)
}

// NextPriorityMessage implements the two-level priority `select` pattern over
// a high-priority channel and a standard channel.
func NextPriorityMessage[T any](ctx context.Context, highCh, stdCh <-chan T) (T, bool) {
	var zero T
	// Tier 1: Non-blocking check on high-priority queue first
	select {
	case msg, ok := <-highCh:
		return msg, ok
	default:
	}

	// Tier 2: Blocking wait on either queue (while still checking ctx.Done)
	select {
	case <-ctx.Done():
		return zero, false
	case msg, ok := <-highCh:
		return msg, ok
	case msg, ok := <-stdCh:
		return msg, ok
	}
}

// PriorityOrderConsumer manages two Kafka consumer group readers (`orders.priority.high`
// and `orders.standard`) and feeds them through the priority drain loop.
type PriorityOrderConsumer struct {
	highReader *kafka.Reader
	stdReader  *kafka.Reader
	highCh     chan PriorityOrderMessage
	stdCh      chan PriorityOrderMessage
	logger     *slog.Logger
}

// NewPriorityOrderConsumer creates a dual-topic Kafka consumer in consumer group `"saga"`.
func NewPriorityOrderConsumer(brokers []string, groupID string, logger *slog.Logger) *PriorityOrderConsumer {
	if groupID == "" {
		groupID = "saga"
	}

	highReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID + "-high",
		Topic:    TopicOrdersPriorityHigh,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  200 * time.Millisecond,
	})

	stdReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID + "-standard",
		Topic:    TopicOrdersStandard,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  200 * time.Millisecond,
	})

	return &PriorityOrderConsumer{
		highReader: highReader,
		stdReader:  stdReader,
		highCh:     make(chan PriorityOrderMessage, 64),
		stdCh:      make(chan PriorityOrderMessage, 64),
		logger:     logger,
	}
}

// Start spawns the background reader goroutines and invokes `handler` in strict priority order.
func (c *PriorityOrderConsumer) Start(ctx context.Context, handler func(ctx context.Context, msg PriorityOrderMessage) error) error {
	go c.pumpReader(ctx, c.highReader, TopicOrdersPriorityHigh, c.highCh)
	go c.pumpReader(ctx, c.stdReader, TopicOrdersStandard, c.stdCh)

	for {
		msg, ok := NextPriorityMessage(ctx, c.highCh, c.stdCh)
		if !ok {
			return ctx.Err()
		}

		if err := handler(ctx, msg); err != nil {
			c.logger.Error("saga handler returned error",
				"order_id", msg.Order.ID(),
				"topic", msg.Topic,
				"error", err,
			)
		}
		if err := msg.Ack(ctx); err != nil && !errors.Is(err, context.Canceled) {
			c.logger.Warn("failed committing kafka offset", "topic", msg.Topic, "error", err)
		}
	}
}

func (c *PriorityOrderConsumer) pumpReader(
	ctx context.Context,
	reader *kafka.Reader,
	topic string,
	out chan<- PriorityOrderMessage,
) {
	for {
		rawMsg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			c.logger.Warn("kafka fetch transient error, retrying...", "topic", topic, "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		var order domain.Order
		if err := json.Unmarshal(rawMsg.Value, &order); err != nil {
			c.logger.Error("discarding malformed order message", "topic", topic, "error", err)
			_ = reader.CommitMessages(ctx, rawMsg)
			continue
		}

		wrapped := PriorityOrderMessage{
			Topic: topic,
			Order: &order,
			ack: func(commitCtx context.Context) error {
				return reader.CommitMessages(commitCtx, rawMsg)
			},
		}

		select {
		case out <- wrapped:
		case <-ctx.Done():
			return
		}
	}
}

// Close shuts down both underlying Kafka readers cleanly.
func (c *PriorityOrderConsumer) Close() error {
	errHigh := c.highReader.Close()
	errStd := c.stdReader.Close()
	return errors.Join(errHigh, errStd)
}

