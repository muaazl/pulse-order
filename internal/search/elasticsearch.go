package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"pulseorder/internal/domain"
)

// ============================================================================
// ARCHITECTURAL TEACHING: INVERTED INDEXES & CQRS READ MODELS (ELASTICSEARCH)
// ============================================================================
// WHY NOT QUERY REDIS OR POSTGRES WITH `LIKE '%truffle%'`?
// 1. Redis is a key-value speed layer optimized for $O(1)$ point lookups
//    (`GET order:{id}`). Scanning thousands of keys to filter by text, zone,
//    status, and minimum total while computing `AVG(surge_multiplier)` blocks
//    Redis's single-threaded event loop.
// 2. Relational `LIKE '%truffle%'` cannot use a standard B-Tree index, forcing
//    expensive sequential scans under high read concurrency.
//
// HOW ELASTICSEARCH SOLVES THIS IN <50ms (CQRS PATTERN):
// By decoupling writes (Redis + Kafka Saga) from analytical reads (Elasticsearch
// `pulse_orders` index):
//   - Full-Text Search (`q`): Uses an **Inverted Index** (Lucene standard
//     analyzer tokenizes SKU codes, item names, user IDs, and promo codes).
//   - Exact Filtering (`zone`, `status`, `min_total`): Uses **Bitsets** cached
//     in the filter context (zero scoring overhead).
//   - Real-Time Aggregations (`count_by_status`, `avg_surge_by_zone`): Uses
//     columnar **Doc Values** to compute facet counts and averages in a single
//     pass alongside the top search hits.
//   - Idempotent Indexing: Indexing via `PUT /pulse_orders/_doc/{order_id}` uses
//     the Order ID as the document `_id`, ensuring at-least-once Kafka replays
//     simply update the existing document without creating duplicates.

const IndexName = "pulse_orders"

// OrderDocument is the denormalized Elasticsearch document schema stored in `pulse_orders`.
type OrderDocument struct {
	OrderID         string             `json:"order_id"`
	UserID          string             `json:"user_id"`
	ZoneID          string             `json:"zone_id"`
	Status          string             `json:"status"`
	PromoCode       string             `json:"promo_code,omitempty"`
	IsPriority      bool               `json:"is_priority"`
	BaseAmount      float64            `json:"base_amount"`
	DiscountAmount  float64            `json:"discount_amount"`
	SurgeMultiplier float64            `json:"surge_multiplier"`
	TotalAmount     float64            `json:"total_amount"`
	Items           []domain.OrderItem `json:"items"`
	SearchText      string             `json:"search_text"`
	CreatedAt       time.Time          `json:"created_at"`
	UpdatedAt       time.Time          `json:"updated_at"`
}

// FromDomainOrder converts an encapsulated domain.Order into an OrderDocument.
func FromDomainOrder(order *domain.Order) OrderDocument {
	items := order.Items()
	var parts []string
	parts = append(parts, order.ID(), order.UserID(), order.ZoneID(), order.PromoCode(), string(order.Status()))
	for _, item := range items {
		parts = append(parts, item.SKU, item.Name)
	}

	return OrderDocument{
		OrderID:         order.ID(),
		UserID:          order.UserID(),
		ZoneID:          order.ZoneID(),
		Status:          string(order.Status()),
		PromoCode:       order.PromoCode(),
		IsPriority:      order.IsPriority(),
		BaseAmount:      order.BaseAmount(),
		DiscountAmount:  order.DiscountAmount(),
		SurgeMultiplier: order.SurgeMultiplier(),
		TotalAmount:     order.TotalAmount(),
		Items:           items,
		SearchText:      strings.Join(parts, " "),
		CreatedAt:       order.CreatedAt,
		UpdatedAt:       order.UpdatedAt,
	}
}

// SearchQuery encapsulates the query parameters for `GET /api/v1/orders/search`.
type SearchQuery struct {
	Query    string
	ZoneID   string
	Status   string
	MinTotal float64
	Limit    int
}

// ZoneSurgeStats holds the aggregated order count and average surge multiplier for a zone.
type ZoneSurgeStats struct {
	OrderCount         int     `json:"order_count"`
	AvgSurgeMultiplier float64 `json:"avg_surge_multiplier"`
}

// SearchResult is the structured response returned by `GET /api/v1/orders/search`.
type SearchResult struct {
	TotalHits      int                       `json:"total_hits"`
	TookMs         int64                     `json:"took_ms"`
	Engine         string                    `json:"engine"` // "elasticsearch" or "in_memory_fallback"
	Orders         []OrderDocument           `json:"orders"`
	CountByStatus  map[string]int            `json:"count_by_status"`
	AvgSurgeByZone map[string]ZoneSurgeStats `json:"avg_surge_by_zone"`
}

// Client manages indexing and search queries against Elasticsearch (`pulse_orders`)
// with an automatic thread-safe in-memory mirror so search remains available
// even if the JVM Elasticsearch container is not running.
type Client struct {
	baseURL    string
	httpClient *http.Client
	logger     *slog.Logger

	mu       sync.RWMutex
	fallback map[string]OrderDocument
}

// NewClient initializes an Elasticsearch client targeting `baseURL` (e.g., `http://localhost:9200`).
func NewClient(baseURL string, logger *slog.Logger) *Client {
	return &Client{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		httpClient: &http.Client{
			Timeout: 3 * time.Second,
		},
		logger:   logger,
		fallback: make(map[string]OrderDocument),
	}
}

// EnsureIndex idempotently creates the `pulse_orders` index with explicit field
// mappings for fast keyword filtering, full-text search, and numeric aggregations.
func (c *Client) EnsureIndex(ctx context.Context) error {
	mapping := `{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "refresh_interval": "1s"
  },
  "mappings": {
    "properties": {
      "order_id":         { "type": "keyword" },
      "user_id":          { "type": "keyword" },
      "zone_id":          { "type": "keyword" },
      "status":           { "type": "keyword" },
      "promo_code":       { "type": "keyword" },
      "is_priority":      { "type": "boolean" },
      "base_amount":      { "type": "double" },
      "discount_amount":  { "type": "double" },
      "surge_multiplier": { "type": "double" },
      "total_amount":     { "type": "double" },
      "search_text":      { "type": "text" },
      "created_at":       { "type": "date" },
      "updated_at":       { "type": "date" },
      "items": {
        "type": "nested",
        "properties": {
          "sku":        { "type": "keyword" },
          "name":       { "type": "text" },
          "quantity":   { "type": "integer" },
          "unit_price": { "type": "double" }
        }
      }
    }
  }
}`

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+"/"+IndexName, strings.NewReader(mapping))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("connect elasticsearch %s: %w", c.baseURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 200 OK (created) or 400 resource_already_exists_exception are both valid
	if resp.StatusCode >= 200 && resp.StatusCode < 300 || resp.StatusCode == http.StatusBadRequest {
		return nil
	}
	body, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("elasticsearch create index returned %d: %s", resp.StatusCode, string(body))
}

// IndexOrder upserts an Order into `pulse_orders` using `order.ID()` as the document `_id`.
func (c *Client) IndexOrder(ctx context.Context, order *domain.Order) error {
	doc := FromDomainOrder(order)

	// Always keep local mirror updated for instant fallback/verification
	c.mu.Lock()
	c.fallback[doc.OrderID] = doc
	c.mu.Unlock()

	payload, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("%s/%s/_doc/%s", c.baseURL, IndexName, doc.OrderID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("index order %s into elasticsearch: %w", doc.OrderID, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("elasticsearch index status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// Search executes a filtered full-text & analytical aggregation query against
// Elasticsearch `pulse_orders` (falling back to the in-memory index if ES is offline).
func (c *Client) Search(ctx context.Context, q SearchQuery) (*SearchResult, error) {
	if q.Limit <= 0 || q.Limit > 100 {
		q.Limit = 25
	}

	dslBytes, err := BuildSearchDSL(q)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/%s/_search", c.baseURL, IndexName)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(dslBytes))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		resp, doErr := c.httpClient.Do(req)
		if doErr == nil {
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				bodyBytes, readErr := io.ReadAll(resp.Body)
				if readErr == nil {
					res, parseErr := parseElasticsearchResponse(bodyBytes)
					if parseErr == nil {
						return res, nil
					}
				}
			}
		}
	}

	// Fallback: Execute the exact same filter + aggregation semantics in-memory
	return c.searchInMemory(q), nil
}

// BuildSearchDSL constructs the Elasticsearch Query DSL with `bool.must`,
// `bool.filter`, and `aggs` (`count_by_status` & `avg_surge_by_zone`).
func BuildSearchDSL(q SearchQuery) ([]byte, error) {
	limit := q.Limit
	if limit <= 0 {
		limit = 25
	}

	var mustClauses []map[string]any
	if trimmedQ := strings.TrimSpace(q.Query); trimmedQ != "" {
		mustClauses = append(mustClauses, map[string]any{
			"multi_match": map[string]any{
				"query":  trimmedQ,
				"fields": []string{"search_text", "order_id", "user_id", "promo_code"},
			},
		})
	} else {
		mustClauses = append(mustClauses, map[string]any{
			"match_all": map[string]any{},
		})
	}

	var filterClauses []map[string]any
	if zone := strings.ToLower(strings.TrimSpace(q.ZoneID)); zone != "" {
		filterClauses = append(filterClauses, map[string]any{
			"term": map[string]any{"zone_id": zone},
		})
	}
	if status := strings.ToUpper(strings.TrimSpace(q.Status)); status != "" {
		filterClauses = append(filterClauses, map[string]any{
			"term": map[string]any{"status": status},
		})
	}
	if q.MinTotal > 0 {
		filterClauses = append(filterClauses, map[string]any{
			"range": map[string]any{
				"total_amount": map[string]any{"gte": q.MinTotal},
			},
		})
	}

	dsl := map[string]any{
		"size": limit,
		"sort": []map[string]any{
			{"created_at": map[string]string{"order": "desc"}},
		},
		"query": map[string]any{
			"bool": map[string]any{
				"must":   mustClauses,
				"filter": filterClauses,
			},
		},
		"aggs": map[string]any{
			"count_by_status": map[string]any{
				"terms": map[string]any{"field": "status", "size": 10},
			},
			"avg_surge_by_zone": map[string]any{
				"terms": map[string]any{"field": "zone_id", "size": 20},
				"aggs": map[string]any{
					"avg_surge": map[string]any{
						"avg": map[string]any{"field": "surge_multiplier"},
					},
				},
			},
		},
	}

	return json.Marshal(dsl)
}

type esSearchResponse struct {
	Took int64 `json:"took"`
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		Hits []struct {
			Source OrderDocument `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
	Aggregations struct {
		CountByStatus struct {
			Buckets []struct {
				Key      string `json:"key"`
				DocCount int    `json:"doc_count"`
			} `json:"buckets"`
		} `json:"count_by_status"`
		AvgSurgeByZone struct {
			Buckets []struct {
				Key      string `json:"key"`
				DocCount int    `json:"doc_count"`
				AvgSurge struct {
					Value float64 `json:"value"`
				} `json:"avg_surge"`
			} `json:"buckets"`
		} `json:"avg_surge_by_zone"`
	} `json:"aggregations"`
}

func parseElasticsearchResponse(body []byte) (*SearchResult, error) {
	var raw esSearchResponse
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	orders := make([]OrderDocument, 0, len(raw.Hits.Hits))
	for _, h := range raw.Hits.Hits {
		orders = append(orders, h.Source)
	}

	countByStatus := make(map[string]int)
	for _, b := range raw.Aggregations.CountByStatus.Buckets {
		countByStatus[b.Key] = b.DocCount
	}

	avgSurgeByZone := make(map[string]ZoneSurgeStats)
	for _, b := range raw.Aggregations.AvgSurgeByZone.Buckets {
		avgSurgeByZone[b.Key] = ZoneSurgeStats{
			OrderCount:         b.DocCount,
			AvgSurgeMultiplier: math.Round(b.AvgSurge.Value*100) / 100,
		}
	}

	return &SearchResult{
		TotalHits:      raw.Hits.Total.Value,
		TookMs:         raw.Took,
		Engine:         "elasticsearch",
		Orders:         orders,
		CountByStatus:  countByStatus,
		AvgSurgeByZone: avgSurgeByZone,
	}, nil
}

func (c *Client) searchInMemory(q SearchQuery) *SearchResult {
	start := time.Now()
	queryLower := strings.ToLower(strings.TrimSpace(q.Query))
	zoneFilter := strings.ToLower(strings.TrimSpace(q.ZoneID))
	statusFilter := strings.ToUpper(strings.TrimSpace(q.Status))

	c.mu.RLock()
	defer c.mu.RUnlock()

	var matched []OrderDocument
	countByStatus := make(map[string]int)
	zoneSums := make(map[string]float64)
	zoneCounts := make(map[string]int)

	for _, doc := range c.fallback {
		if zoneFilter != "" && strings.ToLower(doc.ZoneID) != zoneFilter {
			continue
		}
		if statusFilter != "" && strings.ToUpper(doc.Status) != statusFilter {
			continue
		}
		if q.MinTotal > 0 && doc.TotalAmount < q.MinTotal {
			continue
		}
		if queryLower != "" && !strings.Contains(strings.ToLower(doc.SearchText), queryLower) {
			continue
		}

		matched = append(matched, doc)
		countByStatus[doc.Status]++
		zoneSums[doc.ZoneID] += doc.SurgeMultiplier
		zoneCounts[doc.ZoneID]++
	}

	avgSurgeByZone := make(map[string]ZoneSurgeStats, len(zoneCounts))
	for z, cnt := range zoneCounts {
		avgSurgeByZone[z] = ZoneSurgeStats{
			OrderCount:         cnt,
			AvgSurgeMultiplier: math.Round((zoneSums[z]/float64(cnt))*100) / 100,
		}
	}

	limit := q.Limit
	if limit <= 0 || limit > len(matched) {
		limit = len(matched)
	}

	return &SearchResult{
		TotalHits:      len(matched),
		TookMs:         time.Since(start).Milliseconds(),
		Engine:         "in_memory_fallback",
		Orders:         matched[:limit],
		CountByStatus:  countByStatus,
		AvgSurgeByZone: avgSurgeByZone,
	}
}

