package domain

import (
	_ "embed"
	"encoding/json"
	"strings"
	"sync"
)

//go:embed catalog_data.json
var embeddedCatalogJSON []byte

// WalletSeed represents an initial user wallet balance in the curated dataset.
type WalletSeed struct {
	UserID  string  `json:"user_id"`
	Balance float64 `json:"balance"`
}

// CatalogDataset holds the seed merchants, SKUs, and wallets for PulseOrder.
type CatalogDataset struct {
	Merchants []*Merchant  `json:"merchants"`
	Wallets   []WalletSeed `json:"wallets"`
	skuIndex  map[string]MerchantItem
}

var (
	defaultCatalogOnce sync.Once
	defaultCatalog     *CatalogDataset
)

// DefaultCatalog parses and returns the embedded marketplace dataset.
// Using sync.Once ensures thread-safe, single-initialization across concurrent goroutines.
func DefaultCatalog() *CatalogDataset {
	defaultCatalogOnce.Do(func() {
		var raw struct {
			Merchants []struct {
				ID     string         `json:"id"`
				Name   string         `json:"name"`
				ZoneID string         `json:"zone_id"`
				Items  []MerchantItem `json:"items"`
			} `json:"merchants"`
			Wallets []WalletSeed `json:"wallets"`
		}
		if err := json.Unmarshal(embeddedCatalogJSON, &raw); err != nil {
			panic("failed to parse embedded catalog_data.json: " + err.Error())
		}

		ds := &CatalogDataset{
			Merchants: make([]*Merchant, 0, len(raw.Merchants)),
			Wallets:   raw.Wallets,
			skuIndex:  make(map[string]MerchantItem),
		}

		for _, m := range raw.Merchants {
			merchant, err := NewMerchant(m.ID, m.Name, m.ZoneID, m.Items)
			if err != nil {
				continue
			}
			ds.Merchants = append(ds.Merchants, merchant)
			for _, item := range m.Items {
				ds.skuIndex[strings.ToUpper(strings.TrimSpace(item.SKU))] = item
			}
		}
		defaultCatalog = ds
	})
	return defaultCatalog
}

// LookupSKU returns the catalog MerchantItem for a given SKU code if it exists in the dataset.
func (c *CatalogDataset) LookupSKU(sku string) (MerchantItem, bool) {
	item, ok := c.skuIndex[strings.ToUpper(strings.TrimSpace(sku))]
	return item, ok
}

// AllSKUs returns a slice of all MerchantItems across all seeded merchants.
func (c *CatalogDataset) AllSKUs() []MerchantItem {
	var items []MerchantItem
	for _, m := range c.Merchants {
		items = append(items, m.Items...)
	}
	return items
}

