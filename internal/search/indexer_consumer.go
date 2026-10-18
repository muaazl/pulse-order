package search

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"pulseorder/internal/messaging"
)

// IndexerConsumer reads order lifecycle events from Kafka topic `orders.events`
// and indexes the Order aggregate into Elasticsearch (`pulse_orders`).
type IndexerConsumer struct {
	reader   *kafka.Reader
	esClient *Client
	logger   *slog.Logger
}

// NewIndexerConsumer constructs a Kafka-to-Elasticsearch indexer in consumer group `"es-indexer"`.
func NewIndexerConsumer(brokers []string, esClient *Client, logger *slog.Logger) *IndexerConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  "es-indexer",
		Topic:    messaging.TopicOrdersEvents,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  250 * time.Millisecond,
	})
	return &IndexerConsumer{
		reader:   reader,
		esClient: esClient,
		logger:   logger,
	}
}

// Start runs the indexer loop until `ctx` is cancelled.
func (ic *IndexerConsumer) Start(ctx context.Context) error {
	if err := ic.esClient.EnsureIndex(ctx); err != nil {
		ic.logger.Warn("elasticsearch index initialization warning (will retry on document upsert)",
			"index", IndexName,
			"error", err,
		)
	}

	for {
		msg, err := ic.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			ic.logger.Warn("elasticsearch indexer kafka fetch error, retrying...", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		var env messaging.OrderEventEnvelope
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			ic.logger.Error("malformed OrderEventEnvelope on orders.events", "error", err)
			_ = ic.reader.CommitMessages(ctx, msg)
			continue
		}

		if env.Order != nil {
			if err := ic.esClient.IndexOrder(ctx, env.Order); err != nil {
				ic.logger.Warn("failed indexing order into elasticsearch (stored in local mirror)",
					"order_id", env.Order.ID(),
					"event_type", env.EventType,
					"error", err,
				)
			} else {
				ic.logger.Info("indexed order in elasticsearch",
					"order_id", env.Order.ID(),
					"event_type", env.EventType,
					"status", env.Order.Status(),
					"zone_id", env.Order.ZoneID(),
				)
			}
		}

		_ = ic.reader.CommitMessages(ctx, msg)
	}
}

// Close shuts down the underlying Kafka reader.
func (ic *IndexerConsumer) Close() error {
	return ic.reader.Close()
}
