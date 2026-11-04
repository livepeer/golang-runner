package liverunner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"runtime/debug"
	"slices"
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
	version         string
	runnerID        string
	apps            []appRequest

	client              HTTPDoer
	timeout             time.Duration
	heartbeatInterval   time.Duration
	heartbeatTTL        time.Duration
	intervalOverride    bool
	unregisterOnClose   bool
	onReserve           Callback
	onRelease           Callback
	o2r                 *Channel
	o2rCancel           context.CancelFunc
	closed              bool
	heartbeatLoopCancel context.CancelFunc
	wg                  sync.WaitGroup
}

type heartbeatRequest struct {
	RunnerID string       `json:"runner_id,omitempty"`
	Version  string       `json:"version,omitempty"`
	Apps     []appRequest `json:"apps"`
}

type appRequest struct {
	AppOptions
	SessionIDs []string `json:"session_ids,omitempty"`
}

type appResponse struct {
	ID         string   `json:"id"`
	SessionIDs []string `json:"session_ids"`
}

type heartbeatResponse struct {
	RunnerID          string        `json:"runner_id"`
	Orchestrator      string        `json:"orchestrator,omitempty"`
	HeartbeatInterval string        `json:"heartbeat_interval"`
	HeartbeatTTL      string        `json:"heartbeat_ttl"`
	HeartbeatSecret   string        `json:"heartbeat_secret,omitempty"`
	O2R               *Channel      `json:"o2r,omitempty"`
	Apps              []appResponse `json:"apps"`
}

func New(opts Options) (*Runner, error) {
	base, err := normalizeBase(opts.OrchestratorURL)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(opts.BootstrapSecret) == "" {
		return nil, errors.New("bootstrap secret is required")
	}
	if len(opts.Apps) == 0 {
		return nil, errors.New("at least one app is required")
	}
	apps := make([]appRequest, len(opts.Apps))
	seen := make(map[string]bool, len(apps))
	for i, appOpts := range opts.Apps {
		app, err := newAppRequest(appOpts)
		if err != nil {
			return nil, fmt.Errorf("app %d: %w", i, err)
		}
		if app.ID != "" {
			if seen[app.ID] {
				return nil, fmt.Errorf("duplicate app ID %q", app.ID)
			}
			seen[app.ID] = true
		}
		apps[i] = app
	}
	interval := opts.HeartbeatInterval
	if interval <= 0 {
		interval = defaultHeartbeatInterval
	}
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return &Runner{
		orchestratorURL:   base,
		bootstrapSecret:   opts.BootstrapSecret,
		version:           moduleVersion(),
		runnerID:          strings.TrimSpace(opts.RunnerID),
		apps:              apps,
		client:            opts.HTTPClient,
		timeout:           timeout,
		heartbeatInterval: interval,
		intervalOverride:  opts.HeartbeatInterval > 0,
		unregisterOnClose: opts.UnregisterOnClose || !opts.DisableUnregisterOnClose,
		onReserve:         opts.OnReserve,
		onRelease:         opts.OnRelease,
	}, nil
}

func newAppRequest(opts AppOptions) (appRequest, error) {
	var err error
	opts.Mode, err = normalizeMode(opts.Mode)
	if err != nil {
		return appRequest{}, err
	}
	opts.PriceInfo, err = normalizePriceInfo(opts.PriceInfo)
	if err != nil {
		return appRequest{}, err
	}
	if strings.TrimSpace(opts.RunnerURL) == "" {
		return appRequest{}, errors.New("runner URL is required")
	}
	if strings.TrimSpace(opts.App) == "" {
		return appRequest{}, errors.New("app is required")
	}
	if opts.Capacity <= 0 {
		opts.Capacity = 1
	}
	if opts.Status == "" {
		opts.Status = "ready"
	}
	opts.ID = strings.TrimSpace(opts.ID)
	if opts.GPU != nil {
		gpu := *opts.GPU
		opts.GPU = &gpu
	}
	return appRequest{AppOptions: opts}, nil
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

// Start may be called only once, even if registration fails.
func (r *Runner) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.heartbeatLoopCancel != nil || r.closed {
		r.mu.Unlock()
		return errors.New("runner already started or closed")
	}
	loopCtx, cancel := context.WithCancel(context.Background())
	r.heartbeatLoopCancel = cancel
	r.wg.Add(1)
	r.mu.Unlock()
	if err := r.sendHeartbeat(ctx); err != nil {
		cancel()
		r.wg.Done()
		return err
	}
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
	if r.heartbeatLoopCancel != nil {
		r.heartbeatLoopCancel()
	}
	if r.o2rCancel != nil {
		r.o2rCancel()
	}
	r.mu.Unlock()
	r.wg.Wait()
	if !r.unregisterOnClose || r.runnerID == "" || r.heartbeatSecret == "" {
		return nil
	}
	url, err := joinEndpoint(r.orchestratorURL, "/runners/"+url.PathEscape(r.runnerID)+"/unregister")
	if err != nil {
		return err
	}
	return postEmpty(ctx, r.client, url, map[string]string{"Authorization": r.heartbeatSecret}, r.timeout)
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

// AppIDs returns app IDs in Options.Apps order. Omitted IDs are assigned on registration.
func (r *Runner) AppIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, len(r.apps))
	for i, app := range r.apps {
		ids[i] = app.ID
	}
	return ids
}

// ActiveSessionIDs returns a copy of the sessions tracked for an app.
func (r *Runner) ActiveSessionIDs(appID string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if app := r.appLocked(appID); app != nil {
		return copyStrings(app.SessionIDs)
	}
	return nil
}

func (r *Runner) appLocked(appID string) *appRequest {
	if appID != "" {
		for i := range r.apps {
			if r.apps[i].ID == appID {
				return &r.apps[i]
			}
		}
	}
	return nil
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
	payload := r.payloadLocked()
	r.mu.Unlock()
	url, err := joinEndpoint(base, "/runners/heartbeat")
	if err != nil {
		return nil, err
	}
	var resp heartbeatResponse
	if err := requestJSON(ctx, r.client, http.MethodPost, url, payload, map[string]string{"Authorization": auth}, r.timeout, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (r *Runner) payloadLocked() heartbeatRequest {
	apps := make([]appRequest, len(r.apps))
	for i, app := range r.apps {
		apps[i] = app
		apps[i].SessionIDs = copyStrings(app.SessionIDs)
	}
	return heartbeatRequest{
		RunnerID: r.runnerID,
		Version:  r.version,
		Apps:     apps,
	}
}

func (r *Runner) applyHeartbeatResponse(resp *heartbeatResponse, initial bool) error {
	if resp == nil || strings.TrimSpace(resp.RunnerID) == "" {
		return errors.New("heartbeat response missing runner_id")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(resp.Apps) < len(r.apps) {
		return errors.New("heartbeat response missing apps")
	}
	seen := make(map[string]bool, len(resp.Apps))
	for i, app := range resp.Apps {
		if strings.TrimSpace(app.ID) == "" || seen[app.ID] {
			return errors.New("heartbeat response contains missing or duplicate app IDs")
		}
		seen[app.ID] = true
		if i < len(r.apps) && r.apps[i].ID != "" && r.apps[i].ID != app.ID {
			return fmt.Errorf("heartbeat response changed app ID %q", r.apps[i].ID)
		}
	}
	base := r.orchestratorURL
	if strings.TrimSpace(resp.Orchestrator) != "" {
		var err error
		base, err = normalizeBase(resp.Orchestrator)
		if err != nil {
			return err
		}
	}
	if initial && strings.TrimSpace(resp.HeartbeatSecret) == "" {
		return errors.New("heartbeat response missing heartbeat_secret")
	}
	r.runnerID = strings.TrimSpace(resp.RunnerID)
	r.orchestratorURL = base
	for i := range r.apps {
		r.apps[i].ID = resp.Apps[i].ID
	}
	if !r.intervalOverride {
		r.heartbeatInterval = parseGoDuration(resp.HeartbeatInterval, defaultHeartbeatInterval)
	}
	r.heartbeatTTL = parseGoDuration(resp.HeartbeatTTL, 0)
	if strings.TrimSpace(resp.HeartbeatSecret) != "" {
		r.heartbeatSecret = strings.TrimSpace(resp.HeartbeatSecret)
	}
	if resp.O2R != nil && strings.TrimSpace(resp.O2R.URL) != "" && !r.closed && (r.o2r == nil || r.o2r.URL != resp.O2R.URL) {
		if r.o2rCancel != nil {
			r.o2rCancel()
		}
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
		r.handleO2RMessage(ctx, msg)
	}
}

func (r *Runner) HandleO2RMessage(message map[string]any) {
	r.handleO2RMessage(context.Background(), message)
}

func (r *Runner) handleO2RMessage(ctx context.Context, message map[string]any) {
	if message == nil || message["keep"] == "alive" {
		return
	}
	event, _ := message["event"].(string)
	sessionID, _ := message["session"].(string)
	sessionID = strings.TrimSpace(sessionID)
	appID, _ := message["app_id"].(string)
	appID = strings.TrimSpace(appID)
	if (event != "reserved" && event != "released") || sessionID == "" {
		slog.Warn("ignoring unknown live runner O2R message", "message", message)
		return
	}
	timestamp, _ := message["timestamp"].(string)
	token, _ := message["session_token"].(string)
	ev := SessionEvent{AppID: appID, SessionID: sessionID, SessionToken: token, Event: event, Timestamp: timestamp, Raw: message}
	var cb Callback
	r.mu.Lock()
	if ctx.Err() != nil || r.closed {
		r.mu.Unlock()
		return
	}
	app := r.appLocked(appID)
	if app == nil {
		r.mu.Unlock()
		slog.Warn("ignoring live runner O2R message for unknown app", "app_id", appID)
		return
	}
	if event == "reserved" {
		if !slices.Contains(app.SessionIDs, sessionID) {
			app.SessionIDs = append(app.SessionIDs, sessionID)
		}
		cb = r.onReserve
	} else {
		if i := slices.Index(app.SessionIDs, sessionID); i >= 0 {
			app.SessionIDs = slices.Delete(app.SessionIDs, i, i+1)
		}
		cb = r.onRelease
	}
	r.mu.Unlock()
	if cb != nil {
		cb(ev)
	}
}
