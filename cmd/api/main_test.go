package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"pulseorder/internal/cache"
	"pulseorder/internal/concurrency"
	"pulseorder/internal/config"
	"pulseorder/internal/domain"
)

func setupTestAPIServer(t *testing.T) *httptest.Server {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis failed: %v", err)
	}
	t.Cleanup(func() { mr.Close() })

	rc := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := cache.NewRedisStoreFromClient(rc)
	catalog := domain.DefaultCatalog()
	if err := store.SeedCatalog(context.Background(), catalog); err != nil {
		t.Fatalf("SeedCatalog failed: %v", err)
	}

	cfg := &config.Config{
		HTTPPort:          "8080",
		AdminAPIKey:       "test-admin-key",
		RateLimitRequests: 10,
		RateLimitWindow:   10 * time.Second,
		IdempotencyTTL:    300 * time.Second,
	}

	srv := &APIServer{
		cfg:        cfg,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		redisStore: store,
		surgeCache: cache.NewSurgeCache(rc, nil),
		wallets:    concurrency.NewWalletManager(catalog.Wallets),
		catalog:    catalog,
	}

	ts := httptest.NewServer(srv.Routes())
	t.Cleanup(func() { ts.Close() })
	return ts
}

func TestAPIGateway_EndToEndMilestone2(t *testing.T) {
	ts := setupTestAPIServer(t)
	client := ts.Client()

	// 1. GET /api/v1/catalog -> 200 OK with seeded dataset
	respCat, err := client.Get(ts.URL + "/api/v1/catalog")
	if err != nil {
		t.Fatalf("GET /api/v1/catalog failed: %v", err)
	}
	defer respCat.Body.Close()
	if respCat.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /api/v1/catalog, got %d", respCat.StatusCode)
	}

	// 2. POST /api/v1/orders using dataset SKU "TECH-MBP-14" -> 202 Accepted
	orderPayload := `{"user_id":"user-alice","zone_id":"zone-downtown","items":[{"sku":"TECH-MBP-14","quantity":1}],"promo_code":"SAVE10","is_priority":true}`
	reqOrd, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/orders", bytes.NewBufferString(orderPayload))
	reqOrd.Header.Set("Idempotency-Key", "e2e-idem-001")
	reqOrd.Header.Set("X-User-ID", "user-alice")
	respOrd, err := client.Do(reqOrd)
	if err != nil {
		t.Fatalf("POST /api/v1/orders failed: %v", err)
	}
	defer respOrd.Body.Close()

	if respOrd.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(respOrd.Body)
		t.Fatalf("expected 202 Accepted, got %d: %s", respOrd.StatusCode, string(body))
	}

	var acceptedBody struct {
		OrderID     string  `json:"order_id"`
		StatusURL   string  `json:"status_url"`
		TotalAmount float64 `json:"total_amount"`
	}
	_ = json.NewDecoder(respOrd.Body).Decode(&acceptedBody)
	// TECH-MBP-14 is $1999.00 in catalog_data.json, 10% off SAVE10 = $1799.10
	if acceptedBody.TotalAmount != 1799.10 {
		t.Errorf("expected catalog-enriched TotalAmount 1799.10, got %.2f", acceptedBody.TotalAmount)
	}

	// 3. GET /api/v1/orders/{id} -> 200 OK
	respGet, err := client.Get(ts.URL + acceptedBody.StatusURL)
	if err != nil {
		t.Fatalf("GET %s failed: %v", acceptedBody.StatusURL, err)
	}
	defer respGet.Body.Close()
	if respGet.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on order poll, got %d", respGet.StatusCode)
	}

	// 4. POST /api/v1/wallets/transfer -> 200 OK with deterministic lock order
	transferPayload := `{"from_user_id":"user-bob","to_user_id":"user-alice","amount":125.00}`
	respTx, err := client.Post(ts.URL+"/api/v1/wallets/transfer", "application/json", bytes.NewBufferString(transferPayload))
	if err != nil {
		t.Fatalf("POST /api/v1/wallets/transfer failed: %v", err)
	}
	defer respTx.Body.Close()
	if respTx.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on wallet transfer, got %d", respTx.StatusCode)
	}
}

