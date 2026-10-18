package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"pulseorder/internal/cache"
	"pulseorder/internal/concurrency"
	"pulseorder/internal/config"
	"pulseorder/internal/domain"
	"pulseorder/internal/messaging"
	"pulseorder/internal/middleware"
)

// APIServer wires together the HTTP gateway, Redis speed layer, middlewares,
// Kafka event publisher, and deadlock-free concurrency manager.
type APIServer struct {
	cfg        *config.Config
	logger     *slog.Logger
	redisStore *cache.RedisStore
	surgeCache *cache.SurgeCache
	wallets    *concurrency.WalletManager
	catalog    *domain.CatalogDataset
	publisher  domain.EventPublisher
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg := config.Load()

	// 1. Initialize Redis Speed Layer
	redisStore := cache.NewRedisStore(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	defer func() { _ = redisStore.Close() }()

	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStartup()

	catalog := domain.DefaultCatalog()
	if err := redisStore.Ping(startupCtx); err != nil {
		logger.Warn("redis ping failed at startup (ensure `docker compose up -d` is running)", "addr", cfg.RedisAddr, "error", err)
	} else {
		if err := redisStore.SeedCatalog(startupCtx, catalog); err != nil {
			logger.Error("failed seeding initial catalog into redis", "error", err)
		} else {
			logger.Info("seeded curated marketplace dataset into redis",
				"merchants", len(catalog.Merchants),
				"skus", len(catalog.AllSKUs()),
				"wallets", len(catalog.Wallets),
			)
		}
	}

	// 2. Initialize SurgeCache with singleflight stampede protection
	surgeCache := cache.NewSurgeCache(redisStore.Client(), func(_ context.Context, _ string) (float64, error) {
		return 1.00, nil
	})

	// 3. Initialize Deadlock-Free Wallet Manager seeded with catalog wallets
	walletMgr := concurrency.NewWalletManager(catalog.Wallets)

	// 4. Initialize Kafka Topics (3 partitions) & EventPublisher
	if err := messaging.EnsureTopics(startupCtx, cfg.KafkaBrokers); err != nil {
		logger.Warn("kafka topic initialization skipped at startup (broker will auto-create on publish)",
			"brokers", cfg.KafkaBrokers,
			"error", err,
		)
	}
	kafkaPublisher := messaging.NewKafkaPublisher(cfg.KafkaBrokers, logger)
	defer func() { _ = kafkaPublisher.Close() }()

	apiServer := &APIServer{
		cfg:        cfg,
		logger:     logger,
		redisStore: redisStore,
		surgeCache: surgeCache,
		wallets:    walletMgr,
		catalog:    catalog,
		publisher:  kafkaPublisher,
	}

	httpServer := &http.Server{
		Addr:         ":" + cfg.HTTPPort,
		Handler:      apiServer.Routes(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// 4. Start Server & Listen for Graceful Shutdown Signals (SIGINT / SIGTERM)
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("PulseOrder API Gateway listening",
			"port", cfg.HTTPPort,
			"rate_limit_requests", cfg.RateLimitRequests,
			"rate_limit_window", cfg.RateLimitWindow.String(),
		)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server crashed", "error", err)
			os.Exit(1)
		}
	}()

	<-sigCtx.Done()
	logger.Info("shutdown signal received, draining active HTTP connections...")

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown encountered error", "error", err)
	}
	logger.Info("PulseOrder API Gateway stopped cleanly")
}

// Routes registers all endpoints with their respective Redis speed-layer middlewares.
func (s *APIServer) Routes() http.Handler {
	mux := http.NewServeMux()

	rateLimiter := middleware.NewSlidingWindowRateLimiter(
		s.redisStore.Client(),
		s.cfg.RateLimitRequests,
		s.cfg.RateLimitWindow,
	)
	idempotencyGuard := middleware.NewIdempotencyGuard(
		s.redisStore.Client(),
		s.cfg.IdempotencyTTL,
	)
	adminAuthGuard := middleware.NewAdminAuthGuard(s.cfg.AdminAPIKey)

	// Health & Catalog Endpoints (200 OK)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /api/v1/catalog", s.handleGetCatalog)

	// Synchronous Merchant Creation (201 Created)
	mux.Handle("POST /api/v1/merchants", rateLimiter.Wrap(http.HandlerFunc(s.handleCreateMerchant)))

	// Asynchronous Order Ingestion (202 Accepted)
	// Protected by Middleware 1 (Sliding-Window Rate Limiter -> 429)
	// and Middleware 2 (Redis Idempotency-Key Guard -> SET NX EX)
	orderIngestPipeline := rateLimiter.Wrap(idempotencyGuard.Wrap(http.HandlerFunc(s.handleCreateOrder)))
	mux.Handle("POST /api/v1/orders", orderIngestPipeline)

	// Synchronous Order & Surge Reads (200 OK)
	mux.Handle("GET /api/v1/orders/{id}", rateLimiter.Wrap(http.HandlerFunc(s.handleGetOrder)))
	mux.Handle("GET /api/v1/surge/{zone_id}", rateLimiter.Wrap(http.HandlerFunc(s.handleGetZoneSurge)))

	// Deadlock-Free Concurrency Simulator Endpoints (200 OK)
	mux.Handle("GET /api/v1/wallets", http.HandlerFunc(s.handleGetWallets))
	mux.Handle("POST /api/v1/wallets/transfer", rateLimiter.Wrap(http.HandlerFunc(s.handleWalletTransfer)))

	// Admin Endpoint Protected by 401 Unauthorized / 403 Forbidden Middleware
	mux.Handle("POST /api/v1/admin/stock-reset", adminAuthGuard.Wrap(http.HandlerFunc(s.handleAdminStockReset)))

	return mux
}

// --- HTTP Handlers Demonstrating RFC Status Code Precision ---

func (s *APIServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	redisOK := s.redisStore.Ping(r.Context()) == nil
	writeJSON(w, http.StatusOK, map[string]any{
		"status":          "ok",
		"service":         "pulseorder-api",
		"redis_connected": redisOK,
		"timestamp":       time.Now().UTC(),
	})
}

func (s *APIServer) handleGetCatalog(w http.ResponseWriter, r *http.Request) {
	allSKUs := s.catalog.AllSKUs()
	skuCodes := make([]string, len(allSKUs))
	for i, item := range allSKUs {
		skuCodes[i] = item.SKU
	}
	liveStocks, err := s.redisStore.GetSKUStockMap(r.Context(), skuCodes)
	if err != nil {
		s.logger.Warn("failed fetching live SKU stocks from redis", "error", err)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"merchants":   s.catalog.Merchants,
		"live_stocks": liveStocks,
		"wallets":     s.catalog.Wallets,
	})
}

type createMerchantRequest struct {
	ID     string                `json:"id"`
	Name   string                `json:"name"`
	ZoneID string                `json:"zone_id"`
	Items  []domain.MerchantItem `json:"items"`
}

// handleCreateMerchant returns 201 Created for synchronous resource creation.
func (s *APIServer) handleCreateMerchant(w http.ResponseWriter, r *http.Request) {
	var req createMerchantRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}

	merchant, err := domain.NewMerchant(req.ID, req.Name, req.ZoneID, req.Items)
	if err != nil {
		writeError(w, http.StatusBadRequest, "validation_failed", err.Error())
		return
	}

	if err := s.redisStore.SaveMerchant(r.Context(), merchant); err != nil {
		writeError(w, http.StatusInternalServerError, "persistence_error", err.Error())
		return
	}

	w.Header().Set("Location", "/api/v1/merchants/"+merchant.ID)
	writeJSON(w, http.StatusCreated, merchant)
}

type createOrderRequest struct {
	UserID     string             `json:"user_id"`
	ZoneID     string             `json:"zone_id"`
	Items      []domain.OrderItem `json:"items"`
	PromoCode  string             `json:"promo_code"`
	IsPriority bool               `json:"is_priority"`
}

// handleCreateOrder returns 202 Accepted for asynchronous order ingestion.
func (s *APIServer) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "failed to parse order request JSON: "+err.Error())
		return
	}

	// Allow X-User-ID header to populate user_id if omitted in body
	if strings.TrimSpace(req.UserID) == "" {
		req.UserID = strings.TrimSpace(r.Header.Get("X-User-ID"))
	}

	// 1. Fetch live zone surge multiplier from Redis (guarded by singleflight)
	zoneID := strings.ToLower(strings.TrimSpace(req.ZoneID))
	surgeMultiplier := 1.0
	if domain.ValidZones[zoneID] {
		if mult, _, err := s.surgeCache.GetZoneMultiplier(r.Context(), zoneID); err == nil {
			surgeMultiplier = mult
		}
	}

	// 2. Construct & Validate Order via Fluent Builder Pattern
	order, err := domain.NewOrderBuilder().
		WithUser(req.UserID).
		WithZone(req.ZoneID).
		WithItems(req.Items).
		WithPromoCode(req.PromoCode).
		WithPriority(req.IsPriority).
		WithSurgeMultiplier(surgeMultiplier).
		Build()
	if err != nil {
		writeError(w, http.StatusBadRequest, "order_validation_failed", err.Error())
		return
	}

	// 3. Persist initial ACCEPTED state in Redis Speed Layer
	if err := s.redisStore.Save(r.Context(), order); err != nil {
		s.logger.Error("failed saving initial order to redis", "order_id", order.ID(), "error", err)
		writeError(w, http.StatusInternalServerError, "persistence_failed", "could not persist order state")
		return
	}

	// 4. Publish to Kafka if EventPublisher is configured (wired in Milestone 3)
	if s.publisher != nil {
		if err := s.publisher.PublishOrderAccepted(r.Context(), order); err != nil {
			s.logger.Error("failed publishing order to kafka", "order_id", order.ID(), "error", err)
			writeError(w, http.StatusInternalServerError, "event_publish_failed", "could not enqueue order for saga processing")
			return
		}
	}

	statusURL := fmt.Sprintf("/api/v1/orders/%s", order.ID())
	w.Header().Set("Location", statusURL)
	writeJSON(w, http.StatusAccepted, map[string]any{
		"message":          "Order accepted for asynchronous Saga processing",
		"order_id":         order.ID(),
		"status":           order.Status(),
		"status_url":       statusURL,
		"zone_id":          order.ZoneID(),
		"surge_multiplier": order.SurgeMultiplier(),
		"base_amount":      order.BaseAmount(),
		"discount_amount":  order.DiscountAmount(),
		"total_amount":     order.TotalAmount(),
		"is_priority":      order.IsPriority(),
		"order":            order,
	})
}

func (s *APIServer) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	orderID := strings.TrimSpace(r.PathValue("id"))
	if orderID == "" {
		writeError(w, http.StatusBadRequest, "missing_order_id", "order id path parameter is required")
		return
	}

	order, err := s.redisStore.GetByID(r.Context(), orderID)
	if err != nil {
		if errors.Is(err, cache.ErrOrderNotFound) {
			writeError(w, http.StatusNotFound, "order_not_found", fmt.Sprintf("order %q not found", orderID))
			return
		}
		writeError(w, http.StatusInternalServerError, "redis_error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, order)
}

func (s *APIServer) handleGetZoneSurge(w http.ResponseWriter, r *http.Request) {
	zoneID := strings.ToLower(strings.TrimSpace(r.PathValue("zone_id")))
	if !domain.ValidZones[zoneID] {
		writeError(w, http.StatusBadRequest, "invalid_zone", fmt.Sprintf("zone %q is not a recognized marketplace zone", zoneID))
		return
	}

	mult, shared, err := s.surgeCache.GetZoneMultiplier(r.Context(), zoneID)
	if err != nil {
		s.logger.Warn("surge lookup degraded to default 1.0x", "zone_id", zoneID, "error", err)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"zone_id":               zoneID,
		"surge_multiplier":      mult,
		"singleflight_coalesced": shared,
		"checked_at":            time.Now().UTC(),
	})
}

func (s *APIServer) handleGetWallets(w http.ResponseWriter, r *http.Request) {
	balances, err := s.wallets.SnapshotBalances(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "lock_timeout", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"wallets": balances,
	})
}

type walletTransferRequest struct {
	FromUserID string  `json:"from_user_id"`
	ToUserID   string  `json:"to_user_id"`
	Amount     float64 `json:"amount"`
}

func (s *APIServer) handleWalletTransfer(w http.ResponseWriter, r *http.Request) {
	var req walletTransferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}

	receipt, err := s.wallets.Transfer(r.Context(), req.FromUserID, req.ToUserID, req.Amount)
	if err != nil {
		switch {
		case errors.Is(err, concurrency.ErrSameAccount),
			errors.Is(err, concurrency.ErrInvalidAmount),
			errors.Is(err, concurrency.ErrInsufficientBalance):
			writeError(w, http.StatusBadRequest, "transfer_rejected", err.Error())
		case errors.Is(err, concurrency.ErrAccountNotFound):
			writeError(w, http.StatusNotFound, "account_not_found", err.Error())
		case errors.Is(err, concurrency.ErrLockTimeout):
			writeError(w, http.StatusConflict, "lock_acquisition_timeout", err.Error())
		default:
			writeError(w, http.StatusInternalServerError, "transfer_error", err.Error())
		}
		return
	}

	writeJSON(w, http.StatusOK, receipt)
}

func (s *APIServer) handleAdminStockReset(w http.ResponseWriter, r *http.Request) {
	if err := s.redisStore.SeedCatalog(r.Context(), s.catalog); err != nil {
		writeError(w, http.StatusInternalServerError, "seed_failed", err.Error())
		return
	}
	s.wallets = concurrency.NewWalletManager(s.catalog.Wallets)

	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "reset_complete",
		"merchants_seed": len(s.catalog.Merchants),
		"skus_reset":     len(s.catalog.AllSKUs()),
		"wallets_reset":  len(s.catalog.Wallets),
		"timestamp":      time.Now().UTC(),
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error":   code,
		"message": message,
		"status":  status,
	})
}

