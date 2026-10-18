package search_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"pulseorder/internal/domain"
	"pulseorder/internal/search"
)

func TestElasticsearchClient_DSLAndRESTProtocol(t *testing.T) {
	var capturedSearchDSL map[string]any

	mockES := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/pulse_orders":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"acknowledged":true}`))

		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/pulse_orders/_doc/"):
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"result":"created"}`))

		case r.Method == http.MethodPost && r.URL.Path == "/pulse_orders/_search":
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &capturedSearchDSL)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{
				"took": 4,
				"hits": {
					"total": {"value": 2},
					"hits": [
						{
							"_source": {
								"order_id": "ord-es-1",
								"user_id": "user-alice",
								"zone_id": "zone-downtown",
								"status": "COMPLETED",
								"surge_multiplier": 1.6,
								"total_amount": 3198.40
							}
						}
					]
				},
				"aggregations": {
					"count_by_status": {
						"buckets": [
							{"key": "COMPLETED", "doc_count": 2},
							{"key": "FAILED_COMPENSATED", "doc_count": 1}
						]
					},
					"avg_surge_by_zone": {
						"buckets": [
							{
								"key": "zone-downtown",
								"doc_count": 2,
								"avg_surge": {"value": 1.45}
							}
						]
					}
				}
			}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer mockES.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := search.NewClient(mockES.URL, logger)
	ctx := context.Background()

	if err := client.EnsureIndex(ctx); err != nil {
		t.Fatalf("EnsureIndex failed: %v", err)
	}

	order, err := domain.NewOrderBuilder().
		WithID("ord-es-1").
		WithUser("user-alice").
		WithZone("zone-downtown").
		WithItems([]domain.OrderItem{{SKU: "TECH-MBP-14", Quantity: 1}}).
		WithSurgeMultiplier(1.6).
		Build()
	if err != nil {
		t.Fatalf("OrderBuilder failed: %v", err)
	}
	_ = order.TransitionStatus(domain.StatusCompleted)

	if err := client.IndexOrder(ctx, order); err != nil {
		t.Fatalf("IndexOrder failed: %v", err)
	}

	res, err := client.Search(ctx, search.SearchQuery{
		Query:    "MacBook",
		ZoneID:   "zone-downtown",
		Status:   "COMPLETED",
		MinTotal: 1500.00,
	})
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if res.Engine != "elasticsearch" {
		t.Errorf("expected engine=elasticsearch, got %s", res.Engine)
	}
	if res.TotalHits != 2 || len(res.Orders) != 1 {
		t.Errorf("unexpected hits: total=%d len=%d", res.TotalHits, len(res.Orders))
	}
	if res.CountByStatus["COMPLETED"] != 2 || res.CountByStatus["FAILED_COMPENSATED"] != 1 {
		t.Errorf("unexpected status aggregation: %+v", res.CountByStatus)
	}
	if res.AvgSurgeByZone["zone-downtown"].AvgSurgeMultiplier != 1.45 {
		t.Errorf("expected avg_surge 1.45 for zone-downtown, got %.2f",
			res.AvgSurgeByZone["zone-downtown"].AvgSurgeMultiplier)
	}
	if capturedSearchDSL["aggs"] == nil {
		t.Errorf("expected aggs block in generated Elasticsearch Query DSL")
	}
}

func TestElasticsearchClient_FallbackSearchAndAggregations(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// Point to an closed port so fallback engine is exercised
	client := search.NewClient("http://127.0.0.1:59999", logger)
	ctx := context.Background()

	ord1, _ := domain.NewOrderBuilder().
		WithID("ord-fb-1").
		WithUser("user-alice").
		WithZone("zone-downtown").
		WithItems([]domain.OrderItem{{SKU: "TECH-MBP-14", Quantity: 1}}).
		WithSurgeMultiplier(1.3).
		Build()
	_ = ord1.TransitionStatus(domain.StatusCompleted)

	ord2, _ := domain.NewOrderBuilder().
		WithID("ord-fb-2").
		WithUser("user-bob").
		WithZone("zone-downtown").
		WithItems([]domain.OrderItem{{SKU: "TECH-MBP-14", Quantity: 1}}).
		WithSurgeMultiplier(1.9).
		Build()
	_ = ord2.TransitionStatus(domain.StatusCompleted)

	_ = client.IndexOrder(ctx, ord1)
	_ = client.IndexOrder(ctx, ord2)

	res, err := client.Search(ctx, search.SearchQuery{
		Query:    "MacBook",
		ZoneID:   "zone-downtown",
		Status:   "COMPLETED",
		MinTotal: 1000.0,
	})
	if err != nil {
		t.Fatalf("fallback search failed: %v", err)
	}
	if res.TotalHits != 2 {
		t.Fatalf("expected 2 hits, got %d", res.TotalHits)
	}
	// Average surge across 1.3 and 1.9 = 1.60
	if res.AvgSurgeByZone["zone-downtown"].AvgSurgeMultiplier != 1.60 {
		t.Fatalf("expected average surge 1.60, got %.2f", res.AvgSurgeByZone["zone-downtown"].AvgSurgeMultiplier)
	}
}

