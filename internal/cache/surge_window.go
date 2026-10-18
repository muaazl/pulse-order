package cache

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"pulseorder/internal/domain"
)

// ============================================================================
// ARCHITECTURAL TEACHING: SLIDING WINDOWS (HOP) VS. TUMBLING WINDOWS
// ============================================================================
// WHY A 5-MINUTE SLIDING WINDOW (30-SECOND HOP) FOR SURGE PRICING?
// Suppose 120 orders arrive in `zone-downtown` between 12:00:00 and 12:04:59.
//   - If we used a 5-Minute TUMBLING WINDOW (non-overlapping [12:00–12:05),
//     [12:05–12:10)):
//     At 12:04:59, the count is 120 (2.0x surge). One millisecond later at
//     12:05:00, the tumbling window resets to 0 orders and the surge multiplier
//     cliff-drops from 2.0x to 1.0x — even though demand is still peaking!
//
//   - With a 5-Minute SLIDING WINDOW hopping every 30 seconds:
//     Every evaluation looks back over `[now - 5m, now]`. As orders age past
//     5 minutes, the multiplier smoothly steps down (`2.0x -> 1.6x -> 1.3x -> 1.0x`),
//     eliminating artificial boundary cliffs.
//
// DUAL-ENGINE ARCHITECTURE (APACHE FLINK + GO FALLBACK):
// In production clusters, Apache Flink (`flink-job/surge_window_job.py`) consumes
// `orders.events` from Kafka and writes `SET surge:zone:{zone_id} {mult} EX 120`
// to Redis. When running on a developer laptop without spinning up the JVM Flink
// cluster (`ENABLE_GO_SURGE_ENGINE=true`), `SlidingWindowSurgeEngine` executes
// the exact same 5-minute window / 30-second hop math in Go.

const (
	DefaultSurgeWindowSize = 5 * time.Minute
	DefaultSurgeHopSlide   = 30 * time.Second
	DefaultSurgeRedisTTL   = 120 * time.Second
)

// SurgeGaugeObserver is an optional callback (used by Prometheus in Milestone 6)
// invoked whenever a zone's surge multiplier is updated.
type SurgeGaugeObserver func(zoneID string, multiplier float64)

// CalculateSurgeMultiplier maps the 5-minute active order count in a zone
// to its deterministic Surge Multiplier (PRD Section 3.7):
//   - < 20 orders:   1.0x
//   - 20–50 orders:  1.3x
//   - 51–100 orders: 1.6x
//   - > 100 orders:  2.0x
func CalculateSurgeMultiplier(activeOrders int) float64 {
	switch {
	case activeOrders > 100:
		return 2.0
	case activeOrders >= 51:
		return 1.6
	case activeOrders >= 20:
		return 1.3
	default:
		return 1.0
	}
}

// SlidingWindowSurgeEngine maintains a thread-safe 5-minute sliding window of
// order timestamps per zone and syncs updated multipliers into Redis.
type SlidingWindowSurgeEngine struct {
	mu          sync.Mutex
	zoneEvents  map[string][]time.Time
	windowSize  time.Duration
	hopInterval time.Duration
	surgeCache  *SurgeCache
	logger      *slog.Logger
	observer    SurgeGaugeObserver
}

// NewSlidingWindowSurgeEngine initializes the Go sliding-window surge aggregator.
func NewSlidingWindowSurgeEngine(
	surgeCache *SurgeCache,
	windowSize time.Duration,
	hopInterval time.Duration,
	logger *slog.Logger,
) *SlidingWindowSurgeEngine {
	if windowSize <= 0 {
		windowSize = DefaultSurgeWindowSize
	}
	if hopInterval <= 0 {
		hopInterval = DefaultSurgeHopSlide
	}

	ze := make(map[string][]time.Time, len(domain.ValidZones))
	for z := range domain.ValidZones {
		ze[z] = make([]time.Time, 0, 128)
	}

	return &SlidingWindowSurgeEngine{
		zoneEvents:  ze,
		windowSize:  windowSize,
		hopInterval: hopInterval,
		surgeCache:  surgeCache,
		logger:      logger,
	}
}

// WithGaugeObserver registers a callback for Prometheus metrics (`pulse_active_surge_multiplier`).
func (e *SlidingWindowSurgeEngine) WithGaugeObserver(obs SurgeGaugeObserver) *SlidingWindowSurgeEngine {
	e.observer = obs
	return e
}

// RecordOrder ingests an order timestamp into the zone's 5-minute sliding window,
// evicts expired entries older than `now - windowSize`, and writes the resulting
// surge multiplier to Redis (`SET surge:zone:{zone_id} {multiplier} EX 120`).
func (e *SlidingWindowSurgeEngine) RecordOrder(ctx context.Context, zoneID string, ts time.Time) (float64, int, error) {
	zoneID = strings.ToLower(strings.TrimSpace(zoneID))
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	e.mu.Lock()
	cutoff := ts.Add(-e.windowSize)
	events := e.zoneEvents[zoneID]
	events = append(events, ts)
	validEvents := pruneOlderThan(events, cutoff)
	e.zoneEvents[zoneID] = validEvents
	activeCount := len(validEvents)
	e.mu.Unlock()

	multiplier := CalculateSurgeMultiplier(activeCount)
	err := e.surgeCache.SetZoneMultiplier(ctx, zoneID, multiplier, DefaultSurgeRedisTTL)
	if e.observer != nil {
		e.observer(zoneID, multiplier)
	}
	return multiplier, activeCount, err
}

// EvaluateAllZonesAt runs a single sliding-window hop across all marketplace zones
// as of `now`, evicting expired timestamps and updating Redis (`EX 120`).
func (e *SlidingWindowSurgeEngine) EvaluateAllZonesAt(ctx context.Context, now time.Time) map[string]float64 {
	cutoff := now.Add(-e.windowSize)
	results := make(map[string]float64, len(domain.ValidZones))

	e.mu.Lock()
	for zoneID, events := range e.zoneEvents {
		valid := pruneOlderThan(events, cutoff)
		e.zoneEvents[zoneID] = valid
		results[zoneID] = CalculateSurgeMultiplier(len(valid))
	}
	e.mu.Unlock()

	for zoneID, mult := range results {
		_ = e.surgeCache.SetZoneMultiplier(ctx, zoneID, mult, DefaultSurgeRedisTTL)
		if e.observer != nil {
			e.observer(zoneID, mult)
		}
	}
	return results
}

// Reset clears all in-memory sliding window timestamps and resets all zones to 1.0x.
func (e *SlidingWindowSurgeEngine) Reset(ctx context.Context) {
	e.mu.Lock()
	for z := range domain.ValidZones {
		e.zoneEvents[z] = e.zoneEvents[z][:0]
	}
	e.mu.Unlock()
	e.EvaluateAllZonesAt(ctx, time.Now().UTC())
}

// StartHopLoop runs the periodic 30-second sliding-window hop ticker in a background
// goroutine until `ctx` is cancelled.
func (e *SlidingWindowSurgeEngine) StartHopLoop(ctx context.Context) {
	ticker := time.NewTicker(e.hopInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			results := e.EvaluateAllZonesAt(ctx, now.UTC())
			e.logger.Debug("evaluated 5-minute sliding window surge hop", "multipliers", results)
		}
	}
}

// StartKafkaConsumer consumes `ORDER_ACCEPTED` events from `orders.events` and feeds
// the sliding window (mirroring the Apache Flink job when running in Go-fallback mode).
func (e *SlidingWindowSurgeEngine) StartKafkaConsumer(ctx context.Context, brokers []string, topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  "go-surge-sliding-window",
		Topic:    topic,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  500 * time.Millisecond,
	})
	defer func() { _ = reader.Close() }()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			time.Sleep(1 * time.Second)
			continue
		}

		var env struct {
			EventType  string    `json:"event_type"`
			ZoneID     string    `json:"zone_id"`
			OccurredAt time.Time `json:"occurred_at"`
		}
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			continue
		}
		if env.EventType == "ORDER_ACCEPTED" && env.ZoneID != "" {
			mult, count, _ := e.RecordOrder(ctx, env.ZoneID, env.OccurredAt)
			e.logger.Debug("go sliding window updated zone surge from kafka stream",
				"zone_id", env.ZoneID,
				"window_count_5m", count,
				"surge_multiplier", mult,
			)
		}
	}
}

func pruneOlderThan(events []time.Time, cutoff time.Time) []time.Time {
	keepIdx := 0
	for _, ts := range events {
		if !ts.Before(cutoff) {
			events[keepIdx] = ts
			keepIdx++
		}
	}
	return events[:keepIdx]
}
