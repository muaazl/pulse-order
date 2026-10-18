package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pulseorder/internal/cache"
	"pulseorder/internal/config"
	"pulseorder/internal/messaging"
	"pulseorder/internal/saga"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg := config.Load()

	// 1. Connect to Redis Speed Layer (OrderRepository & InventoryStore)
	redisStore := cache.NewRedisStore(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	defer func() { _ = redisStore.Close() }()

	// 2. Ensure Kafka Topics Exist (3 Partitions each)
	topicCtx, cancelTopics := context.WithTimeout(context.Background(), 10*time.Second)
	if err := messaging.EnsureTopics(topicCtx, cfg.KafkaBrokers); err != nil {
		logger.Warn("could not pre-create kafka topics at startup (will auto-create on write)",
			"brokers", cfg.KafkaBrokers,
			"error", err,
		)
	}
	cancelTopics()

	// 3. Initialize Kafka Event Publisher (for emitting OrderFinalizedEvents to `orders.events`)
	publisher := messaging.NewKafkaPublisher(cfg.KafkaBrokers, logger)
	defer func() { _ = publisher.Close() }()

	// 4. Initialize Payment Gateway Simulator & Saga Orchestrator
	paymentGateway := saga.NewSimulatedPaymentGateway(cfg.PaymentDeclineRate)
	sagaOrchestrator := saga.NewOrderSagaOrchestrator(
		redisStore,
		redisStore,
		paymentGateway,
		publisher,
		logger,
	)

	// 5. Initialize Priority Kafka Consumer (`orders.priority.high` drained before `orders.standard`)
	consumer := messaging.NewPriorityOrderConsumer(cfg.KafkaBrokers, "saga", logger)
	defer func() { _ = consumer.Close() }()

	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("PulseOrder Saga Worker started",
		"brokers", cfg.KafkaBrokers,
		"priority_topic", messaging.TopicOrdersPriorityHigh,
		"standard_topic", messaging.TopicOrdersStandard,
		"payment_decline_rate", cfg.PaymentDeclineRate,
	)

	err := consumer.Start(sigCtx, func(ctx context.Context, msg messaging.PriorityOrderMessage) error {
		logger.Info("processing order via Saga Orchestrator",
			"order_id", msg.Order.ID(),
			"source_topic", msg.Topic,
			"is_priority", msg.Order.IsPriority(),
			"user_id", msg.Order.UserID(),
			"zone_id", msg.Order.ZoneID(),
		)
		return sagaOrchestrator.Execute(ctx, msg.Order)
	})

	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("Saga worker exited with error", "error", err)
		os.Exit(1)
	}
	logger.Info("PulseOrder Saga Worker stopped cleanly")
}

