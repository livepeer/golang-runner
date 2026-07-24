package liverunner

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"
)

const (
	ModePersistent   = "persistent"
	ModeSingleShot   = "single-shot"
	PriceCurrencyUSD = "usd"
	PriceUnitHour    = "hour"
	PriceUnit720p    = "720p"
	PriceUnitFixed   = "fixed"

	HeaderSessionID      = "Livepeer-Session-Id"
	HeaderSessionToken   = "Livepeer-Session-Token"
	HeaderSessionControl = "Livepeer-Session-Control"
	HeaderRunnerRoute    = "Livepeer-Runner-Route"
)

type GPU struct {
	ID     string `json:"id,omitempty"`
	Name   string `json:"name,omitempty"`
	VRAMMB int    `json:"vram_mb,omitempty"`
}

type PriceInfo struct {
	Price    json.Number `json:"price"`
	Currency string      `json:"currency"`
	Unit     string      `json:"unit"`
}

// NewPriceInfo constructs registration-side pricing. Registration prices are
// denominated in USD and use hour, 720p, or fixed units. The orchestrator
// converts them to the payment price advertised through discovery.
func NewPriceInfo(price, currency, unit string) (PriceInfo, error) {
	return normalizePriceInfo(PriceInfo{
		Price:    json.Number(strings.TrimSpace(price)),
		Currency: currency,
		Unit:     unit,
	})
}

func normalizePriceInfo(priceInfo PriceInfo) (PriceInfo, error) {
	price := strings.TrimSpace(priceInfo.Price.String())
	if price == "" {
		price = "0"
	}
	if _, err := json.Marshal(json.Number(price)); err != nil {
		return PriceInfo{}, fmt.Errorf("live runner price must be a valid JSON number: %w", err)
	}
	priceRat, ok := new(big.Rat).SetString(price)
	if !ok || priceRat.Sign() < 0 {
		return PriceInfo{}, fmt.Errorf("live runner price must be a non-negative decimal")
	}
	currency := strings.ToLower(strings.TrimSpace(priceInfo.Currency))
	if currency == "" {
		currency = PriceCurrencyUSD
	}
	if currency != PriceCurrencyUSD {
		return PriceInfo{}, fmt.Errorf("live runner price currency must be %q", PriceCurrencyUSD)
	}
	unit := strings.ToLower(strings.TrimSpace(priceInfo.Unit))
	if unit == "" {
		unit = PriceUnitHour
	}
	switch unit {
	case PriceUnitHour, PriceUnit720p, PriceUnitFixed:
	default:
		return PriceInfo{}, fmt.Errorf("live runner price unit must be %q, %q, or %q", PriceUnitHour, PriceUnit720p, PriceUnitFixed)
	}
	return PriceInfo{Price: json.Number(price), Currency: currency, Unit: unit}, nil
}

type ChannelRequest struct {
	Name     string `json:"name"`
	MimeType string `json:"mime_type"`
}

type Channel struct {
	Name        string `json:"name"`
	ChannelName string `json:"channel_name"`
	URL         string `json:"url"`
	InternalURL string `json:"internal_url,omitempty"`
	MimeType    string `json:"mime_type"`
}

type Proxy struct {
	ProxyID string `json:"proxy_id"`
	URL     string `json:"url"`
}

type SessionEvent struct {
	SessionID string
	Event     string
	Timestamp string
	Raw       map[string]any
}

type Callback func(SessionEvent)

type Options struct {
	OrchestratorURL string
	BootstrapSecret string
	RunnerURL       string
	App             string
	Metadata        string
	Mode            string
	Proxy           bool
	Capacity        int
	Status          string
	Label           string
	GPU             *GPU
	PriceInfo       PriceInfo
	RunnerID        string

	HTTPClient               HTTPDoer
	HeartbeatInterval        time.Duration
	Timeout                  time.Duration
	UnregisterOnClose        bool
	DisableUnregisterOnClose bool
	OnReserve                Callback
	OnRelease                Callback
}

type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}
