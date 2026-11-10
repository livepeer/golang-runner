package liverunner

import (
	"context"
	"encoding/json"
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
	apps            []HeartbeatApp
	sessions        map[string][]string

	client              HTTPDoer
	timeout             time.Duration
	heartbeatInterval   time.Duration
	heartbeatTTL        time.Duration
	intervalOverride    bool
	unregisterOnClose   bool
	onReserve           Callback
	onRelease           Callback
	onHeartbeat         func(HeartbeatEvent)
	seq                 uint64
	o2r                 *Channel
	o2rCancel           context.CancelFunc
	closed              bool
	heartbeatLoopCancel context.CancelFunc
	wg                  sync.WaitGroup
}

func New(opts Options) (*Runner, error) {
	base, err := normalizeBase(opts.OrchestratorURL)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(opts.BootstrapSecret) == "" {
		return nil, errors.New("bootstrap secret is required")
	}
	apps, err := newAppRequests(opts.Apps)
	if err != nil {
		return nil, err
	}
	sessions := make(map[string][]string)
	for _, app := range apps {
		if app.ID != "" {
			sessions[app.ID] = nil
		}
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
		sessions:          sessions,
		onHeartbeat:       opts.OnHeartbeat,
		seq:               opts.InitialSeq,
		orchestratorURL:   base,
		bootstrapSecret:   opts.BootstrapSecret,
		heartbeatSecret:   opts.HeartbeatSecret,
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

func newAppRequests(opts []AppOptions) ([]HeartbeatApp, error) {
	apps := make([]HeartbeatApp, len(opts))
	seen := make(map[string]bool, len(apps))
	for i, options := range opts {
		app, err := newAppRequest(options)
		if err != nil {
			return nil, fmt.Errorf("app %d: %w", i, err)
		}
		if app.ID != "" && seen[app.ID] {
			return nil, fmt.Errorf("duplicate app ID %q", app.ID)
		}
		seen[app.ID] = true
		apps[i] = app
	}
	return apps, nil
}

// SetApps replaces the apps reported in future heartbeats.
func (r *Runner) SetApps(opts []AppOptions) error {
	apps, err := newAppRequests(opts)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("runner closed")
	}
	r.apps = apps
	for _, app := range apps {
		if app.ID != "" {
			if _, exists := r.sessions[app.ID]; !exists {
				r.sessions[app.ID] = nil
			}
		}
	}
	return nil
}

func newAppRequest(opts AppOptions) (HeartbeatApp, error) {
	var err error
	opts.Mode, err = normalizeMode(opts.Mode)
	if err != nil {
		return HeartbeatApp{}, err
	}
	opts.PriceInfo, err = normalizePriceInfo(opts.PriceInfo)
	if err != nil {
		return HeartbeatApp{}, err
	}
	if strings.TrimSpace(opts.RunnerURL) == "" {
		return HeartbeatApp{}, errors.New("runner URL is required")
	}
	if strings.TrimSpace(opts.App) == "" {
		return HeartbeatApp{}, errors.New("app is required")
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
	return HeartbeatApp{AppOptions: opts}, nil
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

// Close stops the heartbeat and unregisters the runner.
func (r *Runner) Close(ctx context.Context) error {
	r.mu.Lock()
	if !r.closed {
		r.closed = true
		if r.heartbeatLoopCancel != nil {
			r.heartbeatLoopCancel()
		}
		if r.o2rCancel != nil {
			r.o2rCancel()
		}
	}
	r.mu.Unlock()
	r.wg.Wait()
	r.mu.Lock()
	base, id, secret := r.orchestratorURL, r.runnerID, r.heartbeatSecret
	unregister := r.unregisterOnClose
	r.mu.Unlock()
	if !unregister || id == "" || secret == "" {
		return nil
	}
	endpoint, err := joinEndpoint(base, "/runners/"+url.PathEscape(id)+"/unregister")
	if err != nil {
		return err
	}
	err = postEmpty(ctx, r.client, endpoint, map[string]string{"Authorization": secret}, r.timeout)
	var httpErr *HTTPError
	if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
		err = nil
	}
	if err == nil {
		r.mu.Lock()
		r.unregisterOnClose = false
		r.mu.Unlock()
	}
	return err
}

func (r *Runner) RunnerID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.runnerID
}

// Runner credential; treat this as sensitive
func (r *Runner) HeartbeatSecret() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.heartbeatSecret
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

// AppIDs returns IDs in the order supplied to SetApps or Options.Apps.
// Omitted IDs are assigned on registration; retained draining apps are excluded.
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
	return copyStrings(r.sessions[appID])
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
	if err := ctx.Err(); err != nil {
		return err
	}
	// Each cycle allows one authentication fallback and one sequence retry.
	fallback, catchup := false, false
	for {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return errors.New("runner closed")
		}
		auth := r.heartbeatSecret
		if auth == "" {
			auth = r.bootstrapSecret
		}
		payload := r.payloadLocked()
		apps := r.apps
		payload.Seq = r.seq
		r.seq++
		base := r.orchestratorURL
		r.mu.Unlock()

		at := time.Now()
		endpoint, err := joinEndpoint(base, "/runners/heartbeat")
		var resp HeartbeatResponse
		if err == nil {
			err = requestJSON(ctx, r.client, http.MethodPost, endpoint, payload, map[string]string{"Authorization": auth}, r.timeout, &resp)
		}
		if err == nil {
			err = r.applyHeartbeatResponse(&resp, payload, apps, auth)
		}
		event := HeartbeatEvent{Request: payload, Credential: auth, At: at, Err: err}
		var httpErr *HTTPError
		retry := false
		if errors.As(err, &httpErr) {
			if httpErr.StatusCode == http.StatusUnauthorized && !fallback && auth != r.bootstrapSecret {
				fallback = true
				r.mu.Lock()
				r.heartbeatSecret = ""
				r.seq = 0
				r.mu.Unlock()
				retry = true
			}
			if httpErr.StatusCode == http.StatusConflict {
				var conflict struct {
					Seq *uint64 `json:"seq"`
				}
				if json.Unmarshal([]byte(httpErr.Body), &conflict) == nil && conflict.Seq != nil {
					event.AcceptedSeq = copySeq(conflict.Seq)
					r.mu.Lock()
					if r.seq <= *conflict.Seq {
						r.seq = *conflict.Seq + 1
					}
					retry = !catchup
					r.mu.Unlock()
					catchup = true
				}
			}
		}
		if err == nil {
			event.Response = &resp
			event.AcceptedSeq = copySeq(resp.Seq)
			event.TTL = parseGoDuration(resp.HeartbeatTTL, 0)
		}
		if r.onHeartbeat != nil {
			r.onHeartbeat(event)
		}
		if !retry || ctx.Err() != nil {
			return err
		}
	}
}

func (r *Runner) payloadLocked() HeartbeatRequest {
	apps := make([]HeartbeatApp, len(r.apps))
	for i, app := range r.apps {
		apps[i] = app
		apps[i].SessionIDs = copyStrings(r.sessions[app.ID])
		if app.GPU != nil {
			gpu := *app.GPU
			apps[i].GPU = &gpu
		}
	}
	return HeartbeatRequest{RunnerID: r.runnerID, Version: r.version, Apps: apps}
}

func (r *Runner) applyHeartbeatResponse(resp *HeartbeatResponse, req HeartbeatRequest, apps []HeartbeatApp, auth string) error {
	if strings.TrimSpace(resp.RunnerID) == "" {
		return errors.New("heartbeat response missing runner_id")
	}
	if req.RunnerID != "" && req.RunnerID != resp.RunnerID {
		return errors.New("heartbeat response changed runner_id")
	}
	// A missing or null array does not confirm that all apps were removed.
	if resp.Apps == nil || len(resp.Apps) < len(req.Apps) {
		return errors.New("heartbeat response missing apps")
	}
	seen := make(map[string]bool, len(resp.Apps))
	for i, app := range resp.Apps {
		if strings.TrimSpace(app.ID) == "" || seen[app.ID] {
			return errors.New("heartbeat response contains missing or duplicate app IDs")
		}
		seen[app.ID] = true
		if i < len(req.Apps) && req.Apps[i].ID != "" && req.Apps[i].ID != app.ID {
			return fmt.Errorf("heartbeat response changed app ID %q", req.Apps[i].ID)
		}
	}
	issued := strings.TrimSpace(resp.HeartbeatSecret)
	if resp.Seq != nil && *resp.Seq != req.Seq {
		return errors.New("heartbeat response did not echo submitted seq")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	base := r.orchestratorURL
	if strings.TrimSpace(resp.Orchestrator) != "" {
		var err error
		base, err = normalizeBase(resp.Orchestrator)
		if err != nil {
			return err
		}
	}
	r.runnerID = strings.TrimSpace(resp.RunnerID)
	r.orchestratorURL = base
	// SetApps may have replaced r.apps while this request was in flight.
	for i := range apps {
		apps[i].ID = resp.Apps[i].ID
	}
	for _, app := range r.apps {
		if app.ID != "" {
			seen[app.ID] = true
		}
	}
	// Keep sessions for removed apps until any queued O2R releases arrive.
	for id, sessions := range r.sessions {
		if !seen[id] && len(sessions) == 0 {
			delete(r.sessions, id)
		}
	}
	for id := range seen {
		if _, exists := r.sessions[id]; !exists {
			r.sessions[id] = nil
		}
	}
	if !r.intervalOverride {
		r.heartbeatInterval = parseGoDuration(resp.HeartbeatInterval, defaultHeartbeatInterval)
	}
	r.heartbeatTTL = parseGoDuration(resp.HeartbeatTTL, 0)
	r.heartbeatSecret = auth
	if issued != "" {
		r.heartbeatSecret = issued
		if issued != auth {
			r.seq = 0
		}
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

func copySeq(seq *uint64) *uint64 {
	if seq == nil {
		return nil
	}
	value := *seq
	return &value
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
	sessions, known := r.sessions[appID]
	if !known {
		r.mu.Unlock()
		slog.Warn("ignoring live runner O2R message for unknown app", "app_id", appID)
		return
	}
	if event == "reserved" {
		if !slices.Contains(sessions, sessionID) {
			sessions = append(sessions, sessionID)
		}
		cb = r.onReserve
	} else {
		if i := slices.Index(sessions, sessionID); i >= 0 {
			sessions = slices.Delete(sessions, i, i+1)
		}
		cb = r.onRelease
	}
	r.sessions[appID] = sessions
	r.mu.Unlock()
	if cb != nil {
		cb(ev)
	}
}
