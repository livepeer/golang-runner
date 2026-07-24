package liverunner

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

const (
	defaultHeartbeatInterval = 5 * time.Second
	defaultTimeout           = 5 * time.Second
	modulePath               = "github.com/livepeer/golang-runner"
)

type Runner struct {
	mu sync.Mutex

	orchestratorURL string
	bootstrapSecret string
	heartbeatSecret string
	runnerURL       string
	app             string
	metadata        string
	mode            string
	proxy           bool
	capacity        int
	status          string
	label           string
	version         string
	gpu             *GPU
	priceInfo       PriceInfo
	runnerID        string

	client              HTTPDoer
	timeout             time.Duration
	heartbeatInterval   time.Duration
	heartbeatTTL        time.Duration
	intervalOverride    bool
	unregisterOnClose   bool
	onReserve           Callback
	onRelease           Callback
	activeSessionIDs    []string
	o2r                 *Channel
	o2rCancel           context.CancelFunc
	closed              bool
	started             bool
	heartbeatLoopCancel context.CancelFunc
	wg                  sync.WaitGroup
}

type heartbeatRequest struct {
	RunnerID   string    `json:"runner_id,omitempty"`
	Label      string    `json:"label,omitempty"`
	RunnerURL  string    `json:"runner_url"`
	Version    string    `json:"version,omitempty"`
	Metadata   string    `json:"metadata,omitempty"`
	Status     string    `json:"status,omitempty"`
	Mode       string    `json:"mode,omitempty"`
	Proxy      bool      `json:"proxy,omitempty"`
	GPU        *GPU      `json:"gpu,omitempty"`
	App        string    `json:"app"`
	Capacity   int       `json:"capacity"`
	PriceInfo  PriceInfo `json:"price_info"`
	SessionIDs []string  `json:"session_ids,omitempty"`
}

type heartbeatResponse struct {
	RunnerID          string   `json:"runner_id"`
	Orchestrator      string   `json:"orchestrator,omitempty"`
	HeartbeatInterval string   `json:"heartbeat_interval"`
	HeartbeatTTL      string   `json:"heartbeat_ttl"`
	HeartbeatSecret   string   `json:"heartbeat_secret,omitempty"`
	O2R               *Channel `json:"o2r,omitempty"`
	SessionIDs        []string `json:"session_ids"`
}

func New(opts Options) (*Runner, error) {
	base, err := normalizeBase(opts.OrchestratorURL)
	if err != nil {
		return nil, err
	}
	mode, err := normalizeMode(opts.Mode)
	if err != nil {
		return nil, err
	}
	priceInfo, err := normalizePriceInfo(opts.PriceInfo)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(opts.BootstrapSecret) == "" {
		return nil, errors.New("bootstrap secret is required")
	}
	if strings.TrimSpace(opts.RunnerURL) == "" {
		return nil, errors.New("runner URL is required")
	}
	if strings.TrimSpace(opts.App) == "" {
		return nil, errors.New("app is required")
	}
	capacity := opts.Capacity
	if capacity <= 0 {
		capacity = 1
	}
	status := opts.Status
	if status == "" {
		status = "ready"
	}
	interval := opts.HeartbeatInterval
	override := interval > 0
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	unregisterOnClose := true
	if opts.DisableUnregisterOnClose {
		unregisterOnClose = false
	}
	if opts.UnregisterOnClose {
		unregisterOnClose = true
	}
	return &Runner{
		orchestratorURL:   base,
		bootstrapSecret:   opts.BootstrapSecret,
		runnerURL:         opts.RunnerURL,
		app:               opts.App,
		metadata:          opts.Metadata,
		mode:              mode,
		proxy:             opts.Proxy,
		capacity:          capacity,
		status:            status,
		label:             opts.Label,
		version:           moduleVersion(),
		gpu:               opts.GPU,
		priceInfo:         priceInfo,
		runnerID:          strings.TrimSpace(opts.RunnerID),
		client:            opts.HTTPClient,
		timeout:           timeout,
		heartbeatInterval: interval,
		intervalOverride:  override,
		unregisterOnClose: unregisterOnClose,
		onReserve:         opts.OnReserve,
		onRelease:         opts.OnRelease,
	}, nil
}

func moduleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	return buildVersion(info)
}

func buildVersion(info *debug.BuildInfo) string {
	if info.Main.Path == modulePath {
		return getVersion(info.Main.Version, info.Settings)
	}
	for _, dep := range info.Deps {
		if dep.Path == modulePath {
			return getVersion(dep.Version, info.Settings)
		}
	}
	return ""
}

func getVersion(version string, settings []debug.BuildSetting) string {
	if version != "" && version != "(devel)" {
		return version
	}
	revision := ""
	modified := false
	for _, setting := range settings {
		switch setting.Key {
		case "vcs.revision":
			revision = setting.Value
		case "vcs.modified":
			modified = setting.Value == "true"
		}
	}
	if revision == "" {
		return ""
	}
	if modified {
		return revision + "-dirty"
	}
	return revision
}

func Register(ctx context.Context, opts Options) (*Runner, error) {
	r, err := New(opts)
	if err != nil {
		return nil, err
	}
	if err := r.Start(ctx); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Runner) Start(ctx context.Context) error {
	if err := r.sendHeartbeat(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	if r.started || r.closed {
		r.mu.Unlock()
		return nil
	}
	loopCtx, cancel := context.WithCancel(context.Background())
	r.heartbeatLoopCancel = cancel
	r.started = true
	r.wg.Add(1)
	r.mu.Unlock()
	go r.heartbeatLoop(loopCtx)
	return nil
}

func (r *Runner) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	hbCancel := r.heartbeatLoopCancel
	o2rCancel := r.o2rCancel
	r.heartbeatLoopCancel = nil
	r.o2rCancel = nil
	r.mu.Unlock()
	if hbCancel != nil {
		hbCancel()
	}
	if o2rCancel != nil {
		o2rCancel()
	}
	r.wg.Wait()

	r.mu.Lock()
	runnerID := r.runnerID
	secret := r.heartbeatSecret
	unregister := r.unregisterOnClose
	base := r.orchestratorURL
	timeout := r.timeout
	client := r.client
	r.mu.Unlock()
	if !unregister || runnerID == "" || secret == "" {
		return nil
	}
	url, err := joinEndpoint(base, "/runners/"+url.PathEscape(runnerID)+"/unregister")
	if err != nil {
		return err
	}
	return postEmpty(ctx, client, url, map[string]string{"Authorization": secret}, timeout)
}

func (r *Runner) RunnerID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runnerID
}

func (r *Runner) OrchestratorURL() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.orchestratorURL
}

func (r *Runner) HeartbeatInterval() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.heartbeatInterval
}

func (r *Runner) HeartbeatTTL() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.heartbeatTTL
}

func (r *Runner) O2RChannel() *Channel {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.o2r == nil {
		return nil
	}
	ch := *r.o2r
	return &ch
}

func (r *Runner) ActiveSessionIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return copyStrings(r.activeSessionIDs)
}

func (r *Runner) heartbeatLoop(ctx context.Context) {
	defer r.wg.Done()
	for {
		interval := r.HeartbeatInterval()
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if err := r.sendHeartbeat(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("live runner heartbeat failed", "err", err)
		}
	}
}

func (r *Runner) sendHeartbeat(ctx context.Context) error {
	r.mu.Lock()
	initial := r.heartbeatSecret == ""
	auth := r.heartbeatSecret
	if auth == "" {
		auth = r.bootstrapSecret
	}
	r.mu.Unlock()
	data, err := r.postHeartbeat(ctx, auth)
	var httpErr *HTTPError
	if err != nil && !initial && errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusUnauthorized {
		r.mu.Lock()
		r.heartbeatSecret = ""
		r.mu.Unlock()
		initial = true
		data, err = r.postHeartbeat(ctx, r.bootstrapSecret)
	}
	if err != nil {
		return err
	}
	return r.applyHeartbeatResponse(data, initial)
}

func (r *Runner) postHeartbeat(ctx context.Context, auth string) (*heartbeatResponse, error) {
	r.mu.Lock()
	base := r.orchestratorURL
	timeout := r.timeout
	client := r.client
	payload := r.payloadLocked()
	r.mu.Unlock()
	url, err := joinEndpoint(base, "/runners/heartbeat")
	if err != nil {
		return nil, err
	}
	var resp heartbeatResponse
	if err := requestJSON(ctx, client, http.MethodPost, url, payload, map[string]string{"Authorization": auth}, timeout, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (r *Runner) payloadLocked() heartbeatRequest {
	return heartbeatRequest{
		RunnerID:   r.runnerID,
		Label:      r.label,
		RunnerURL:  r.runnerURL,
		Version:    r.version,
		Metadata:   r.metadata,
		Status:     r.status,
		Mode:       r.mode,
		Proxy:      r.proxy,
		GPU:        r.gpu,
		App:        r.app,
		Capacity:   r.capacity,
		PriceInfo:  r.priceInfo,
		SessionIDs: copyStrings(r.activeSessionIDs),
	}
}

func (r *Runner) applyHeartbeatResponse(resp *heartbeatResponse, initial bool) error {
	if resp == nil || strings.TrimSpace(resp.RunnerID) == "" {
		return errors.New("heartbeat response missing runner_id")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runnerID = strings.TrimSpace(resp.RunnerID)
	if strings.TrimSpace(resp.Orchestrator) != "" {
		base, err := normalizeBase(resp.Orchestrator)
		if err != nil {
			return err
		}
		r.orchestratorURL = base
	}
	if !r.intervalOverride {
		r.heartbeatInterval = parseGoDuration(resp.HeartbeatInterval, defaultHeartbeatInterval)
	}
	r.heartbeatTTL = parseGoDuration(resp.HeartbeatTTL, 0)
	if strings.TrimSpace(resp.HeartbeatSecret) != "" {
		r.heartbeatSecret = strings.TrimSpace(resp.HeartbeatSecret)
	} else if initial {
		return errors.New("heartbeat response missing heartbeat_secret")
	}
	if initial && resp.O2R != nil && strings.TrimSpace(resp.O2R.URL) != "" && r.o2r == nil && !r.closed {
		ch := *resp.O2R
		r.o2r = &ch
		o2rCtx, cancel := context.WithCancel(context.Background())
		r.o2rCancel = cancel
		r.wg.Add(1)
		go r.readO2R(o2rCtx, ch.URL)
	}
	return nil
}

func (r *Runner) readO2R(ctx context.Context, channelURL string) {
	defer r.wg.Done()
	start := SequenceStart(0)
	subscriber, err := NewTrickleSubscriber(TrickleSubscriberConfig{URL: channelURL, Ctx: ctx, Start: &start})
	if err != nil {
		slog.Warn("live runner O2R reader failed to start", "err", err)
		return
	}
	reader := NewJSONReader(subscriber)
	for {
		msg, err := reader.Next()
		if err != nil {
			if ctx.Err() == nil {
				slog.Warn("live runner O2R reader stopped", "err", err)
			}
			return
		}
		if msg == nil {
			return
		}
		r.HandleO2RMessage(msg)
	}
}

func (r *Runner) HandleO2RMessage(message map[string]any) {
	if message == nil || message["keep"] == "alive" {
		return
	}
	event, _ := message["event"].(string)
	sessionID, _ := message["session"].(string)
	sessionID = strings.TrimSpace(sessionID)
	if (event != "reserved" && event != "released") || sessionID == "" {
		slog.Warn("ignoring unknown live runner O2R message", "message", message)
		return
	}
	timestamp, _ := message["timestamp"].(string)
	ev := SessionEvent{SessionID: sessionID, Event: event, Timestamp: timestamp, Raw: message}
	var cb Callback
	r.mu.Lock()
	if event == "reserved" {
		exists := false
		for _, id := range r.activeSessionIDs {
			if id == sessionID {
				exists = true
				break
			}
		}
		if !exists {
			r.activeSessionIDs = append(r.activeSessionIDs, sessionID)
		}
		cb = r.onReserve
	} else {
		for i, id := range r.activeSessionIDs {
			if id == sessionID {
				r.activeSessionIDs = append(r.activeSessionIDs[:i], r.activeSessionIDs[i+1:]...)
				break
			}
		}
		cb = r.onRelease
	}
	r.mu.Unlock()
	if cb != nil {
		cb(ev)
	}
}
