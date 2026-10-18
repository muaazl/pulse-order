package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"pulseorder/internal/domain"
)

// ============================================================================
// ARCHITECTURAL TEACHING: KAFKA PARTITION KEYS & PRIORITY QUEUING
// ============================================================================
// 1. WHY PARTITION KEYS MATTER (`user_id` vs `zone_id`):
//    Kafka only guarantees strict message ordering WITHIN a single partition,
//    NOT across an entire topic. If we published messages with a nil/round-robin
//    key across our 3 partitions:
//      - Partition 0 might receive `OrderCancelled` for `user-alice`
//      - Partition 2 might receive `OrderCreated` for `user-alice`
//      - Consumer 0 processes `OrderCancelled` BEFORE Consumer 2 processes `OrderCreated`!
//
//    By setting the Partition Key using `Hash(key) % num_partitions`:
//      - On `orders.priority.high` and `orders.standard`, we set `Key = order.UserID()`.
//        Every order for `user-alice` lands on the exact same partition and is
//        processed sequentially by a single Saga worker instance.
//      - On `orders.events` (consumed by Apache Flink for surge pricing), we set
//        `Key = order.ZoneID()`. All telemetry events for `zone-downtown` are
//        co-located on the same partition, minimizing network shuffle during
//        Flink's 5-minute sliding window `keyBy(zone_id)` aggregation.
//
// 2. PRIORITY QUEUING VS. HARD RATE LIMITING:
//    During flash sales, standard order traffic can spike 10x. Instead of
//    letting VIP/Priority orders starve behind 50,000 standard messages in a
//    single FIFO topic, we split ingestion into two topics:
//      - `orders.priority.high` (`is_priority == true`)
//      - `orders.standard`      (`is_priority == false`)
//    In `PriorityOrderConsumer`, our Go worker uses a two-level `select`
//    statement to drain `orders.priority.high` before pulling from `orders.standard`.

const (
	TopicOrdersPriorityHigh = "orders.priority.high"
	TopicOrdersStandard     = "orders.standard"
	TopicOrdersEvents       = "orders.events"

	DefaultPartitions        = 3
	DefaultReplicationFactor = 1
)

// OrderEventEnvelope wraps an Order state transition published to `orders.events`
// for downstream consumers (Apache Flink surge engine & Elasticsearch indexer).
type OrderEventEnvelope struct {
	EventType       string        `json:"event_type"`
	OrderID         string        `json:"order_id"`
	UserID          string        `json:"user_id"`
	ZoneID          string        `json:"zone_id"`
	Status          string        `json:"status"`
	SurgeMultiplier float64       `json:"surge_multiplier"`
	TotalAmount     float64       `json:"total_amount"`
	IsPriority      bool          `json:"is_priority"`
	OccurredAt      time.Time     `json:"occurred_at"`
	Order           *domain.Order `json:"order"`
}

// EnsureTopics connects to the Kafka cluster and idempotently creates our 3
// topics with 3 partitions each (`orders.priority.high`, `orders.standard`, `orders.events`).
func EnsureTopics(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return errors.New("no kafka brokers configured")
	}

	dialer := &kafka.Dialer{Timeout: 4 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka broker %s: %w", brokers[0], err)
	}
	defer func() { _ = conn.Close() }()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("lookup kafka controller: %w", err)
	}

	ctrlAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	ctrlConn, err := dialer.DialContext(ctx, "tcp", ctrlAddr)
	if err != nil {
		// In Docker bridge setups where controller advertises container hostname,
		// fall back to the initial bootstrap broker connection.
		ctrlConn = conn
	} else {
		defer func() { _ = ctrlConn.Close() }()
	}

	topics := []string{
		TopicOrdersPriorityHigh,
		TopicOrdersStandard,
		TopicOrdersEvents,
	}

	configs := make([]kafka.TopicConfig, len(topics))
	for i, t := range topics {
		configs[i] = kafka.TopicConfig{
			Topic:             t,
			NumPartitions:     DefaultPartitions,
			ReplicationFactor: DefaultReplicationFactor,
		}
	}

	return ctrlConn.CreateTopics(configs...)
}

// KafkaPublisher implements domain.EventPublisher using hash-partitioned writers.
type KafkaPublisher struct {
	writer *kafka.Writer
	logger *slog.Logger
}

// NewKafkaPublisher creates a pooled Kafka producer using Hash balancer (murmur2-style key hashing).
func NewKafkaPublisher(brokers []string, logger *slog.Logger) *KafkaPublisher {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Balancer:               &kafka.Hash{}, // Guarantees same Partition Key -> same Partition
		RequiredAcks:           kafka.RequireOne,
		BatchTimeout:           10 * time.Millisecond,
		WriteTimeout:           5 * time.Second,
		AllowAutoTopicCreation: true,
	}
	return &KafkaPublisher{
		writer: w,
		logger: logger,
	}
}

// PublishOrderAccepted routes the accepted order to `orders.priority.high` or
// `orders.standard` (partitioned by `user_id`), and simultaneously emits an
// `ORDER_ACCEPTED` telemetry event to `orders.events` (partitioned by `zone_id`).
func (kp *KafkaPublisher) PublishOrderAccepted(ctx context.Context, order *domain.Order) error {
	targetTopic := TopicOrdersStandard
	if order.IsPriority() {
		targetTopic = TopicOrdersPriorityHigh
	}

	orderBytes, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("marshal order for kafka: %w", err)
	}

	// Message 1: Work queue message for the Saga Worker (Partition Key = user_id)
	workMsg := kafka.Message{
		Topic: targetTopic,
		Key:   []byte(order.UserID()),
		Value: orderBytes,
		Headers: []kafka.Header{
			{Key: "order_id", Value: []byte(order.ID())},
			{Key: "zone_id", Value: []byte(order.ZoneID())},
			{Key: "is_priority", Value: []byte(strconv.FormatBool(order.IsPriority()))},
		},
		Time: time.Now().UTC(),
	}

	// Message 2: Lifecycle event for Flink Sliding-Window Surge & ES (Partition Key = zone_id)
	envBytes, err := marshalEventEnvelope("ORDER_ACCEPTED", order)
	if err != nil {
		return err
	}
	eventMsg := kafka.Message{
		Topic: TopicOrdersEvents,
		Key:   []byte(order.ZoneID()),
		Value: envBytes,
		Time:  time.Now().UTC(),
	}

	if err := kp.writer.WriteMessages(ctx, workMsg, eventMsg); err != nil {
		return fmt.Errorf("write kafka messages to %s: %w", targetTopic, err)
	}

	kp.logger.Info("published order to kafka",
		"order_id", order.ID(),
		"topic", targetTopic,
		"partition_key_user", order.UserID(),
		"partition_key_zone", order.ZoneID(),
		"is_priority", order.IsPriority(),
	)
	return nil
}

// PublishOrderEvent emits a state transition (`ORDER_COMPLETED` or `ORDER_FAILED_COMPENSATED`)
// to `orders.events` partitioned by `zone_id`.
func (kp *KafkaPublisher) PublishOrderEvent(ctx context.Context, eventType string, order *domain.Order) error {
	envBytes, err := marshalEventEnvelope(eventType, order)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Topic: TopicOrdersEvents,
		Key:   []byte(order.ZoneID()),
		Value: envBytes,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(eventType)},
			{Key: "user_id", Value: []byte(order.UserID())},
		},
		Time: time.Now().UTC(),
	}

	return kp.writer.WriteMessages(ctx, msg)
}

// Close flushes pending batches and closes the underlying Kafka writer.
func (kp *KafkaPublisher) Close() error {
	return kp.writer.Close()
}

func marshalEventEnvelope(eventType string, order *domain.Order) ([]byte, error) {
	env := OrderEventEnvelope{
		EventType:       eventType,
		OrderID:         order.ID(),
		UserID:          order.UserID(),
		ZoneID:          order.ZoneID(),
		Status:          string(order.Status()),
		SurgeMultiplier: order.SurgeMultiplier(),
		TotalAmount:     order.TotalAmount(),
		IsPriority:      order.IsPriority(),
		OccurredAt:      time.Now().UTC(),
		Order:           order,
	}
	return json.Marshal(env)
}

