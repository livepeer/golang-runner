package liverunner

import (
	"net/http"
	"time"
)

const (
	ModePersistent = "persistent"
	ModeSingleShot = "single-shot"

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
	PricePerUnit  int64  `json:"price_per_unit"`
	PixelsPerUnit int64  `json:"pixels_per_unit"`
	Unit          string `json:"unit,omitempty"`
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
	Mode            string
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
