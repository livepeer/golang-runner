package liverunner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewPriceInfo(t *testing.T) {
	tests := []struct {
		name     string
		price    string
		currency string
		unit     string
		want     PriceInfo
		wantErr  string
	}{
		{
			name: "defaults",
			want: PriceInfo{
				Price:    json.Number("0"),
				Currency: PriceCurrencyUSD,
				Unit:     PriceUnitHour,
			},
		},
		{
			name:     "normalizes casing and whitespace",
			price:    " 12.50 ",
			currency: " USD ",
			unit:     " FIXED ",
			want: PriceInfo{
				Price:    json.Number("12.50"),
				Currency: PriceCurrencyUSD,
				Unit:     PriceUnitFixed,
			},
		},
		{
			name:    "rejects negative price",
			price:   "-0.01",
			wantErr: "non-negative decimal",
		},
		{
			name:    "rejects malformed price",
			price:   "not-a-number",
			wantErr: "valid JSON number",
		},
		{
			name:     "rejects unsupported currency",
			price:    "1",
			currency: "eur",
			wantErr:  `currency must be "usd"`,
		},
		{
			name:    "rejects unsupported unit",
			price:   "1",
			unit:    "minute",
			wantErr: `unit must be "hour", "720p", or "fixed"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewPriceInfo(tt.price, tt.currency, tt.unit)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("NewPriceInfo() error = %v, want error containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewPriceInfo() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("NewPriceInfo() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestRunnerAppValidation(t *testing.T) {
	valid := AppOptions{App: "echo", RunnerURL: "https://runner.example"}
	for _, tt := range []struct {
		name string
		apps []AppOptions
		want string
	}{
		{"missing app", []AppOptions{valid, {RunnerURL: valid.RunnerURL}}, "app 1: app is required"},
		{"missing URL", []AppOptions{valid, {App: valid.App}}, "app 1: runner URL is required"},
		{"invalid mode", []AppOptions{valid, {App: valid.App, RunnerURL: valid.RunnerURL, Mode: "single_shot"}}, "app 1: live runner mode"},
		{"invalid price", []AppOptions{valid, {App: valid.App, RunnerURL: valid.RunnerURL, PriceInfo: PriceInfo{Price: "-1"}}}, "app 1: live runner price"},
		{"duplicate IDs", []AppOptions{
			{ID: "echo", App: valid.App, RunnerURL: valid.RunnerURL},
			{ID: " echo ", App: valid.App, RunnerURL: valid.RunnerURL},
		}, "duplicate app ID"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(Options{OrchestratorURL: "https://orchestrator.example", BootstrapSecret: "secret", Apps: tt.apps})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("New() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestBuildVersionUsesModuleVersionOrVCSRevision(t *testing.T) {
	tests := []struct {
		name string
		info debug.BuildInfo
		want string
	}{
		{
			name: "main module release",
			info: debug.BuildInfo{
				Main: debug.Module{Path: modulePath, Version: "v1.2.3"},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: "abc123"},
				},
			},
			want: "v1.2.3",
		},
		{
			name: "main module devel falls back to revision",
			info: debug.BuildInfo{
				Main: debug.Module{Path: modulePath, Version: "(devel)"},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: "abc123"},
				},
			},
			want: "abc123",
		},
		{
			name: "main module devel falls back to dirty revision",
			info: debug.BuildInfo{
				Main: debug.Module{Path: modulePath, Version: "(devel)"},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: "abc123"},
					{Key: "vcs.modified", Value: "true"},
				},
			},
			want: "abc123-dirty",
		},
		{
			name: "dependency module release",
			info: debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app", Version: "(devel)"},
				Deps: []*debug.Module{
					{Path: modulePath, Version: "v0.4.0"},
				},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: "appsha"},
				},
			},
			want: "v0.4.0",
		},
		{
			name: "dependency module devel falls back to revision",
			info: debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app", Version: "(devel)"},
				Deps: []*debug.Module{
					{Path: modulePath, Version: "(devel)"},
				},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: "appsha"},
				},
			},
			want: "appsha",
		},
		{
			name: "dependency module devel falls back to dirty revision",
			info: debug.BuildInfo{
				Main: debug.Module{Path: "example.com/app", Version: "(devel)"},
				Deps: []*debug.Module{
					{Path: modulePath, Version: "(devel)"},
				},
				Settings: []debug.BuildSetting{
					{Key: "vcs.revision", Value: "appsha"},
					{Key: "vcs.modified", Value: "true"},
				},
			},
			want: "appsha-dirty",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildVersion(&tt.info); got != tt.want {
				t.Fatalf("buildVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRunnerRegistrationHeartbeatO2RAndUnregister(t *testing.T) {
	ctx := context.Background()
	trickle := newTestTrickleServer(t)
	defer trickle.Close()

	var mu sync.Mutex
	var heartbeatAuth []string
	var payloads []HeartbeatRequest
	var unregistered bool
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/runners/heartbeat":
			var payload HeartbeatRequest
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode payload: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			heartbeatAuth = append(heartbeatAuth, r.Header.Get("Authorization"))
			payloads = append(payloads, payload)
			mu.Unlock()
			json.NewEncoder(w).Encode(HeartbeatResponse{
				RunnerID:          "runner-1",
				Orchestrator:      "http://" + r.Host + "/api",
				HeartbeatInterval: "50ms",
				HeartbeatTTL:      "250ms",
				HeartbeatSecret:   "heartbeat-secret",
				Apps:              []HeartbeatAppStatus{{ID: "echo"}},
				O2R: &Channel{
					Name:        "o2r",
					ChannelName: "runner-1-o2r",
					URL:         trickle.URL("runner-1-o2r"),
					MimeType:    "application/json",
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/runners/runner-1/unregister":
			if got := r.Header.Get("Authorization"); got != "heartbeat-secret" {
				t.Errorf("unregister auth = %q", got)
			}
			mu.Lock()
			unregistered = true
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer orch.Close()

	var reserved []string
	var released []string
	reg, err := Register(ctx, Options{
		OrchestratorURL: orch.URL + "/api",
		BootstrapSecret: "bootstrap-secret",
		Apps: []AppOptions{{
			RunnerURL: "http://runner.example/app",
			App:       "livepeer-sample/echo",
		}},
		UnregisterOnClose: true,
		OnReserve: func(ev SessionEvent) {
			mu.Lock()
			defer mu.Unlock()
			reserved = append(reserved, ev.SessionID)
		},
		OnRelease: func(ev SessionEvent) {
			mu.Lock()
			defer mu.Unlock()
			released = append(released, ev.SessionID)
		},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if reg.RunnerID() != "runner-1" {
		t.Fatalf("runner id = %q", reg.RunnerID())
	}
	if reg.OrchestratorURL() != orch.URL+"/api" {
		t.Fatalf("orchestrator = %q", reg.OrchestratorURL())
	}
	if reg.HeartbeatInterval() != 50*time.Millisecond {
		t.Fatalf("heartbeat interval = %s", reg.HeartbeatInterval())
	}
	if reg.HeartbeatTTL() != 250*time.Millisecond {
		t.Fatalf("heartbeat ttl = %s", reg.HeartbeatTTL())
	}

	tricklePublisher, err := NewTricklePublisher(TricklePublisherConfig{URL: trickle.URL("runner-1-o2r"), ContentType: "application/json"})
	if err != nil {
		t.Fatal(err)
	}
	pub := NewJSONWriter(tricklePublisher)
	defer func() {
		if err := pub.Close(); err != nil {
			t.Errorf("close O2R publisher: %v", err)
		}
	}()
	if err := pub.Write(map[string]any{"keep": "alive"}); err != nil {
		t.Fatal(err)
	}
	if err := pub.Write(map[string]any{"event": "reserved", "app_id": "echo", "session": "session-b"}); err != nil {
		t.Fatal(err)
	}
	if err := pub.Write(map[string]any{"event": "reserved", "app_id": "echo", "session": "session-a"}); err != nil {
		t.Fatal(err)
	}
	if err := pub.Write(map[string]any{"event": "reserved", "app_id": "echo", "session": "session-b"}); err != nil {
		t.Fatal(err)
	}
	if err := pub.Write(map[string]any{"event": "released", "app_id": "echo", "session": "session-b"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(payloads) >= 2 && len(released) == 1 && reflect.DeepEqual(reg.ActiveSessionIDs("echo"), []string{"session-a"})
	})
	if err := reg.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(reserved, []string{"session-b", "session-a", "session-b"}) {
		t.Fatalf("reserved callbacks = %#v", reserved)
	}
	if !reflect.DeepEqual(released, []string{"session-b"}) {
		t.Fatalf("released callbacks = %#v", released)
	}

	if !unregistered {
		t.Fatal("expected unregister")
	}
	if heartbeatAuth[0] != "bootstrap-secret" {
		t.Fatalf("initial heartbeat auth = %#v", heartbeatAuth)
	}
	if heartbeatAuth[1] != "heartbeat-secret" || payloads[1].Apps[0].ID != "echo" {
		t.Fatalf("background heartbeat lost registration: auth=%v, payload=%#v", heartbeatAuth, payloads[1])
	}
	if len(payloads) == 0 || len(payloads[0].Apps) != 1 || payloads[0].Apps[0].RunnerURL != "http://runner.example/app" || payloads[0].Apps[0].App != "livepeer-sample/echo" {
		t.Fatalf("bad heartbeat payload: %#v", payloads)
	}
}

func TestRunnerMultiAppHeartbeatsAndSessions(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var payloads []HeartbeatRequest
	var auth []string
	var unregisters int
	var runner *Runner
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if req.URL.Path == "/api/runners/owner/unregister" {
			unregisters++
			if req.Header.Get("Authorization") != "fresh-secret" {
				t.Errorf("unregister credential = %q", req.Header.Get("Authorization"))
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if req.Method != http.MethodPost || req.URL.Path != "/api/runners/heartbeat" {
			t.Errorf("unexpected request %s %s", req.Method, req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var fields map[string]json.RawMessage
		if err := json.NewDecoder(req.Body).Decode(&fields); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(fields) != 4 || fields["runner_id"] == nil || fields["version"] == nil || fields["apps"] == nil || fields["seq"] == nil {
			t.Errorf("heartbeat top-level fields = %v", fields)
		}
		var rawApps []map[string]json.RawMessage
		if err := json.Unmarshal(fields["apps"], &rawApps); err != nil || len(rawApps) != 2 {
			t.Errorf("heartbeat apps = %s, error = %v", fields["apps"], err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if rawApps[1]["metadata"] != nil || rawApps[1]["proxy"] != nil {
			t.Errorf("empty metadata and disabled proxy should be omitted: %s", fields["apps"])
		}
		data, _ := json.Marshal(fields)
		var payload HeartbeatRequest
		if err := json.Unmarshal(data, &payload); err != nil {
			t.Error(err)
		}
		payloads = append(payloads, payload)
		auth = append(auth, req.Header.Get("Authorization"))
		if len(payloads) == 3 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		secret := ""
		if len(payloads) == 1 {
			secret = "heartbeat-secret"
		}
		if len(payloads) == 4 {
			secret = "fresh-secret"
		}
		if len(payloads) == 2 {
			// An event arriving during the heartbeat must survive its stale response.
			runner.HandleO2RMessage(map[string]any{"event": "reserved", "app_id": "generated", "session": "new"})
		}
		json.NewEncoder(w).Encode(HeartbeatResponse{
			RunnerID: "owner", HeartbeatSecret: secret,
			Apps: []HeartbeatAppStatus{{ID: "explicit", SessionIDs: []string{"stale"}}, {ID: "generated"}},
		})
	}))
	defer orch.Close()

	apps := []AppOptions{
		{
			ID: "explicit", App: "repeated-name", RunnerURL: "https://runner.example/first", Version: "app-v1",
			Metadata: `{"region":"west"}`, Mode: " persistent ", Proxy: true, Capacity: 3, Status: "warm",
			GPU:       &GPU{ID: "gpu-1", Name: "NVIDIA", VRAMMB: 24576},
			PriceInfo: PriceInfo{Price: "12.5", Currency: " USD ", Unit: " FIXED "},
		},
		{App: "repeated-name", RunnerURL: "https://runner.example/second", Version: "app-v2", Mode: ModeSingleShot},
	}
	var events []SessionEvent
	var eventsMu sync.Mutex
	callback := func(event SessionEvent) {
		// Accessors must be usable from callbacks, outside the runner lock.
		_ = runner.ActiveSessionIDs(event.AppID)
		eventsMu.Lock()
		defer eventsMu.Unlock()
		events = append(events, event)
	}
	var err error
	runner, err = New(Options{
		OrchestratorURL: orch.URL + "/api", BootstrapSecret: "bootstrap", RunnerID: "owner",
		Apps: apps, OnReserve: callback, OnRelease: callback,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runner.Close(ctx) })
	runner.version = "sdk-v1"
	// Caller mutation cannot change the runner's startup configuration.
	apps[0].GPU.Name = "changed"
	apps[0].RunnerURL = "https://changed.example"
	apps[1].App = "changed"
	if err := runner.sendHeartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if got := runner.AppIDs(); !reflect.DeepEqual(got, []string{"explicit", "generated"}) {
		t.Fatalf("app IDs = %v", got)
	}
	ids := runner.AppIDs()
	ids[0] = "changed"
	if runner.AppIDs()[0] != "explicit" {
		t.Fatal("AppIDs returned mutable runner state")
	}
	for _, appID := range []string{"explicit", "generated", "explicit"} {
		runner.HandleO2RMessage(map[string]any{
			"event": "reserved", "app_id": appID, "session": "shared", "session_token": "token", "timestamp": "now",
		})
	}
	runner.HandleO2RMessage(map[string]any{"event": "released", "app_id": "explicit", "session": "shared"})
	for _, appID := range []string{"", "unknown"} {
		runner.HandleO2RMessage(map[string]any{"event": "reserved", "app_id": appID, "session": "ignored"})
	}
	eventsMu.Lock()
	gotEvents := append([]SessionEvent(nil), events...)
	eventsMu.Unlock()
	if len(gotEvents) != 4 || gotEvents[0].AppID != "explicit" || gotEvents[1].AppID != "generated" || gotEvents[2].AppID != "explicit" || gotEvents[3].Event != "released" {
		t.Fatalf("callbacks = %#v", gotEvents)
	}
	if gotEvents[0].SessionToken != "token" || gotEvents[0].Timestamp != "now" || gotEvents[0].Raw["session"] != "shared" {
		t.Fatalf("callback details = %#v", gotEvents[0])
	}
	if got := runner.ActiveSessionIDs("explicit"); len(got) != 0 {
		t.Fatalf("released app sessions = %v", got)
	}
	if got := runner.ActiveSessionIDs("unknown"); len(got) != 0 {
		t.Fatalf("unknown app sessions = %v", got)
	}
	sessions := runner.ActiveSessionIDs("generated")
	if !reflect.DeepEqual(sessions, []string{"shared"}) {
		t.Fatalf("other app sessions = %v", sessions)
	}
	sessions[0] = "changed"
	if err := runner.sendHeartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if got := runner.ActiveSessionIDs("generated"); !reflect.DeepEqual(got, []string{"shared", "new"}) {
		t.Fatalf("sessions after stale heartbeat = %v", got)
	}
	if err := runner.sendHeartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if err := runner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if unregisters != 1 || !reflect.DeepEqual(auth, []string{"bootstrap", "heartbeat-secret", "heartbeat-secret", "bootstrap"}) {
		t.Fatalf("unregisters = %d, heartbeat credentials = %v", unregisters, auth)
	}
	for i, payload := range payloads {
		if payload.RunnerID != "owner" || payload.Version != "sdk-v1" || len(payload.Apps) != 2 {
			t.Fatalf("heartbeat %d = %#v", i, payload)
		}
		first, second := payload.Apps[0], payload.Apps[1]
		if first.App != "repeated-name" || second.App != first.App || first.Version != "app-v1" || second.Version != "app-v2" {
			t.Fatalf("app names/versions = %#v", payload.Apps)
		}
		if first.RunnerURL != "https://runner.example/first" || first.Mode != ModePersistent || !first.Proxy || first.Capacity != 3 || first.Status != "warm" || first.Metadata != `{"region":"west"}` || first.GPU.Name != "NVIDIA" || first.PriceInfo != (PriceInfo{Price: "12.5", Currency: "usd", Unit: "fixed"}) {
			t.Fatalf("first app configuration = %#v", first)
		}
		if second.RunnerURL != "https://runner.example/second" || second.Mode != ModeSingleShot || second.Proxy || second.Capacity != 1 || second.Status != "ready" || second.GPU != nil || second.PriceInfo != (PriceInfo{Price: "0", Currency: "usd", Unit: "hour"}) {
			t.Fatalf("second app defaults = %#v", second)
		}
		if first.ID != "explicit" || (i == 0 && second.ID != "") || (i > 0 && second.ID != "generated") {
			t.Fatalf("heartbeat %d lost assigned app IDs: %#v", i, payload.Apps)
		}
		if len(first.SessionIDs) != 0 || (i == 1 && !reflect.DeepEqual(second.SessionIDs, []string{"shared"})) {
			t.Fatalf("heartbeat %d session isolation = %#v", i, payload.Apps)
		}
	}
}

func TestHeartbeatRejectsInvalidAppResponsesAtomically(t *testing.T) {
	for _, tt := range []struct {
		name string
		body string
	}{
		{"missing apps", `{"runner_id":"owner","heartbeat_secret":"secret"}`},
		{"null apps", `{"runner_id":"owner","heartbeat_secret":"secret","apps":null}`},
		{"missing entry", `{"runner_id":"owner","heartbeat_secret":"secret","apps":[{"id":"generated"}]}`},
		{"empty ID", `{"runner_id":"owner","heartbeat_secret":"secret","apps":[{"id":"generated"},{"id":" "}]}`},
		{"duplicate ID", `{"runner_id":"owner","heartbeat_secret":"secret","apps":[{"id":"explicit"},{"id":"explicit"}]}`},
		{"changed explicit ID", `{"runner_id":"owner","heartbeat_secret":"secret","apps":[{"id":"generated"},{"id":"changed"}]}`},
		{"invalid orchestrator", `{"runner_id":"owner","heartbeat_secret":"secret","orchestrator":"file:///bad","apps":[{"id":"generated"},{"id":"explicit"}]}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) { fmt.Fprint(w, tt.body) }))
			defer orch.Close()
			runner, err := New(Options{
				OrchestratorURL: orch.URL, BootstrapSecret: "bootstrap",
				Apps: []AppOptions{{App: "echo", RunnerURL: "https://runner.example"}, {ID: "explicit", App: "echo", RunnerURL: "https://runner.example"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer runner.Close(context.Background())
			if err := runner.Start(context.Background()); err == nil {
				t.Fatal("expected invalid heartbeat response error")
			}
			if err := runner.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "already started") {
				t.Fatalf("Start after failed registration = %v", err)
			}
			if runner.RunnerID() != "" || runner.heartbeatSecret != "" || runner.OrchestratorURL() != orch.URL || !reflect.DeepEqual(runner.AppIDs(), []string{"", "explicit"}) {
				t.Fatal("invalid response partially changed runner state")
			}
		})
	}
}

func TestRunnerCloseWaitsForStartup(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var requests []string
	release := make(chan struct{}, 1)
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		requests = append(requests, req.URL.Path)
		mu.Unlock()
		if req.URL.Path == "/runners/heartbeat" {
			<-release
			json.NewEncoder(w).Encode(HeartbeatResponse{RunnerID: "owner", HeartbeatSecret: "secret", Apps: []HeartbeatAppStatus{{ID: "generated"}}})
			return
		}
		if req.URL.Path != "/runners/owner/unregister" || req.Header.Get("Authorization") != "secret" {
			t.Errorf("unexpected unregister: %s, auth=%q", req.URL.Path, req.Header.Get("Authorization"))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer orch.Close()
	runner, err := New(Options{OrchestratorURL: orch.URL, BootstrapSecret: "bootstrap", Apps: []AppOptions{{App: "echo", RunnerURL: "https://runner.example"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close(ctx)
	defer close(release)
	startResult := make(chan error, 1)
	go func() {
		startResult <- runner.Start(ctx)
	}()
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(requests) != 0
	})
	if err := runner.Start(ctx); err == nil {
		t.Fatal("second Start must not send another registration")
	}
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- runner.Close(ctx)
	}()
	waitFor(t, func() bool {
		runner.mu.Lock()
		defer runner.mu.Unlock()
		return runner.closed
	})
	if len(closeResult) != 0 {
		t.Error("Close returned before registration completed")
	}
	release <- struct{}{}
	waitFor(t, func() bool { return len(startResult) != 0 && len(closeResult) != 0 })
	if err := <-startResult; err != nil {
		t.Fatal(err)
	}
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(requests, []string{"/runners/heartbeat", "/runners/owner/unregister"}) {
		t.Fatalf("startup/shutdown requests = %v", requests)
	}
}

func TestRunnerReplacesO2RChannel(t *testing.T) {
	ctx := context.Background()
	trickle := newTestTrickleServer(t)
	defer trickle.Close()
	var mu sync.Mutex
	channel := "first"
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		json.NewEncoder(w).Encode(HeartbeatResponse{
			RunnerID: "owner", HeartbeatSecret: "secret", Apps: []HeartbeatAppStatus{{ID: "a"}, {ID: "b"}},
			O2R: &Channel{URL: trickle.URL(channel)},
		})
	}))
	defer orch.Close()
	callback := make(chan SessionEvent, 4)
	runner, err := New(Options{
		OrchestratorURL: orch.URL, BootstrapSecret: "bootstrap", DisableUnregisterOnClose: true,
		Apps:      []AppOptions{{App: "echo", RunnerURL: "https://runner.example/a"}, {App: "echo", RunnerURL: "https://runner.example/b"}},
		OnReserve: func(event SessionEvent) { callback <- event },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close(ctx)
	publish := func(name string, seq int, appID, sessionID string) {
		t.Helper()
		message := map[string]any{"event": "reserved", "app_id": appID, "session": sessionID}
		if err := requestJSON(ctx, nil, http.MethodPost, fmt.Sprintf("%s/%d", trickle.URL(name), seq), message, nil, time.Second, nil); err != nil {
			t.Fatal(err)
		}
	}
	waitEvent := func(appID, sessionID string) {
		t.Helper()
		select {
		case event := <-callback:
			if event.AppID != appID || event.SessionID != sessionID {
				t.Fatalf("event = %#v", event)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for O2R event")
		}
	}
	if err := runner.sendHeartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	publish("first", 0, "a", "before")
	waitEvent("a", "before")
	mu.Lock()
	channel = "second"
	mu.Unlock()
	if err := runner.sendHeartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if runner.O2RChannel().URL != trickle.URL("second") {
		t.Fatal("O2R channel was not replaced")
	}
	publish("first", 1, "a", "stale")
	publish("second", 0, "b", "after")
	waitEvent("b", "after")
	staleCtx, cancel := context.WithCancel(ctx)
	cancel()
	runner.handleO2RMessage(staleCtx, map[string]any{"event": "reserved", "app_id": "a", "session": "stale"})
	if err := runner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if got := runner.ActiveSessionIDs("a"); !reflect.DeepEqual(got, []string{"before"}) {
		t.Fatalf("retired channel changed sessions: %v", got)
	}
	if len(callback) != 0 {
		t.Fatal("retired channel invoked callback")
	}
}

func TestHeadersProxyChannelsAndStopUseSessionControl(t *testing.T) {
	ctx := context.Background()
	var seen []string
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path+" token="+r.Header.Get(HeaderSessionToken))
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/runner/runner-1/session/session-1/proxy":
			var payload createProxyRequest
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode proxy payload: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if payload.TargetURL != "http://runner.example.com:7860/app" {
				t.Errorf("proxy target_url = %q", payload.TargetURL)
			}
			json.NewEncoder(w).Encode(Proxy{
				ProxyID: "proxy-1",
				URL:     "https://proxy.example.com",
			})
		case r.Method == http.MethodPost && r.URL.Path == "/runner/runner-1/session/session-1/channels":
			json.NewEncoder(w).Encode(createChannelsResponse{Channels: []Channel{{
				Name:        "events",
				ChannelName: "chan-events",
				URL:         "http://trickle/chan-events",
				InternalURL: "http://internal/chan-events",
				MimeType:    "application/jsonl",
			}}})
		case r.Method == http.MethodDelete && r.URL.Path == "/runner/runner-1/session/session-1/channels":
			json.NewEncoder(w).Encode(deleteChannelsResponse{Deleted: []string{"events"}})
		case r.Method == http.MethodPost && r.URL.Path == "/runner/runner-1/session/session-1/stop":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer orch.Close()
	req := httptest.NewRequest(http.MethodGet, "http://runner/app", nil)
	req.Header.Set(HeaderSessionToken, "token-1")
	req.Header.Set(HeaderSessionControl, orch.URL+"/runner/runner-1/session/session-1")
	if SessionToken(req) != "token-1" || SessionControl(req) != orch.URL+"/runner/runner-1/session/session-1" {
		t.Fatal("header helpers returned unexpected values")
	}
	proxy, err := CreateProxy(ctx, nil, "", req, "http://runner.example.com:7860/app")
	if err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	if proxy.ProxyID != "proxy-1" || proxy.URL != "https://proxy.example.com" {
		t.Fatalf("proxy = %#v", proxy)
	}
	channels, err := CreateTrickleChannels(ctx, nil, "", req, []ChannelRequest{{Name: "events", MimeType: "application/jsonl"}})
	if err != nil {
		t.Fatalf("create channels: %v", err)
	}
	if channels[0].InternalURL == "" || channels[0].ChannelName != "chan-events" {
		t.Fatalf("channels = %#v", channels)
	}
	deleted, err := DeleteTrickleChannels(ctx, nil, "", req, []string{"events"})
	if err != nil {
		t.Fatalf("delete channels: %v", err)
	}
	if !reflect.DeepEqual(deleted, []string{"events"}) {
		t.Fatalf("deleted = %#v", deleted)
	}
	if err := StopSession(ctx, nil, req, time.Second); err != nil {
		t.Fatalf("stop session: %v", err)
	}
	want := []string{
		"POST /runner/runner-1/session/session-1/proxy token=token-1",
		"POST /runner/runner-1/session/session-1/channels token=token-1",
		"DELETE /runner/runner-1/session/session-1/channels token=token-1",
		"POST /runner/runner-1/session/session-1/stop token=token-1",
	}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("seen = %#v", seen)
	}
}

func TestRunnerSessionOperationsUseAppRoutes(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var paths []string
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		paths = append(paths, req.Method+" "+req.URL.Path)
		if req.Header.Get(HeaderSessionToken) != "token" {
			t.Errorf("session token = %q", req.Header.Get(HeaderSessionToken))
		}
		switch {
		case strings.HasSuffix(req.URL.Path, "/proxy"):
			var payload map[string]json.RawMessage
			if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if payload["target_url"] != nil {
				t.Errorf("default proxy must omit target_url: %v", payload)
			}
			json.NewEncoder(w).Encode(Proxy{ProxyID: "proxy", URL: "https://proxy.example"})
		case req.Method == http.MethodDelete:
			json.NewEncoder(w).Encode(deleteChannelsResponse{Deleted: []string{"data"}})
		case strings.HasSuffix(req.URL.Path, "/channels"):
			json.NewEncoder(w).Encode(createChannelsResponse{Channels: []Channel{{Name: "data"}}})
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer orch.Close()
	runner, err := New(Options{
		OrchestratorURL: orch.URL + "/api", BootstrapSecret: "bootstrap", RunnerID: "owner",
		Apps: []AppOptions{{ID: "a", App: "echo", RunnerURL: "https://runner.example/a"}, {ID: "b", App: "echo", RunnerURL: "https://runner.example/b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, appID := range []string{"a", "b"} {
		req := httptest.NewRequest(http.MethodPost, "https://runner.example", nil)
		req.Header.Set(HeaderSessionID, "session")
		req.Header.Set(HeaderSessionToken, "token")
		req.Header.Set(HeaderRunnerRoute, appID)
		if _, err := runner.CreateProxy(ctx, req, ""); err != nil {
			t.Fatal(err)
		}
		if _, err := runner.CreateTrickleChannels(ctx, req, []ChannelRequest{{Name: "data", MimeType: "application/json"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := runner.DeleteTrickleChannels(ctx, req, []string{"data"}); err != nil {
			t.Fatal(err)
		}
		if err := runner.StopSession(ctx, appID, "session", "token"); err != nil {
			t.Fatal(err)
		}
	}
	want := []string{
		"POST /api/runner/a/session/session/proxy", "POST /api/runner/a/session/session/channels",
		"DELETE /api/runner/a/session/session/channels", "POST /api/runner/a/session/session/stop",
		"POST /api/runner/b/session/session/proxy", "POST /api/runner/b/session/session/channels",
		"DELETE /api/runner/b/session/session/channels", "POST /api/runner/b/session/session/stop",
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(paths, want) {
		t.Fatalf("session routes = %v, want %v", paths, want)
	}
	req := httptest.NewRequest(http.MethodPost, "https://runner.example", nil)
	req.Header.Set(HeaderSessionID, "session")
	req.Header.Set(HeaderSessionToken, "token")
	req.Header.Set(HeaderRunnerRoute, "b")
	req.Header.Set(HeaderSessionControl, orch.URL+"/control")
	endpoint, _, err := sessionEndpoint("", req, "proxy")
	if err != nil || endpoint != orch.URL+"/control/proxy" {
		t.Fatalf("control URL precedence = %q, %v", endpoint, err)
	}
	req.Header.Del(HeaderSessionControl)
	req.Header.Del(HeaderRunnerRoute)
	if _, err := runner.CreateProxy(ctx, req, ""); err == nil {
		t.Fatal("missing app route must not fall back to the owning runner ID")
	}
	if err := runner.StopSession(ctx, "", "session", "token"); err == nil {
		t.Fatal("missing stop app ID must fail")
	}
}
func waitFor(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

type testTrickleServer struct {
	*httptest.Server
	mu       sync.Mutex
	cond     *sync.Cond
	channels map[string]map[int][]byte
	closed   map[string]bool
}

func newTestTrickleServer(t *testing.T) *testTrickleServer {
	t.Helper()
	ts := &testTrickleServer{
		channels: map[string]map[int][]byte{},
		closed:   map[string]bool{},
	}
	ts.cond = sync.NewCond(&ts.mu)
	ts.Server = httptest.NewServer(http.HandlerFunc(ts.handle))
	return ts
}

func (ts *testTrickleServer) URL(channel string) string {
	return ts.Server.URL + "/trickle/" + channel
}

func (ts *testTrickleServer) handle(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 2 || parts[0] != "trickle" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	channel := parts[1]
	switch r.Method {
	case http.MethodPost:
		if len(parts) != 3 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		seq, _ := strconv.Atoi(parts[2])
		data, _ := io.ReadAll(r.Body)
		ts.mu.Lock()
		if ts.channels[channel] == nil {
			ts.channels[channel] = map[int][]byte{}
		}
		ts.channels[channel][seq] = data
		ts.cond.Broadcast()
		ts.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		if len(parts) != 3 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		seq, _ := strconv.Atoi(parts[2])
		deadline := time.Now().Add(2 * time.Second)
		for {
			ts.mu.Lock()
			if data, ok := ts.channels[channel][seq]; ok {
				latest := seq
				for got := range ts.channels[channel] {
					if got > latest {
						latest = got
					}
				}
				ts.mu.Unlock()
				w.Header().Set("Lp-Trickle-Seq", strconv.Itoa(seq))
				w.Header().Set("Lp-Trickle-Latest", strconv.Itoa(latest))
				w.Write(data)
				return
			}
			if ts.closed[channel] || time.Now().After(deadline) {
				ts.mu.Unlock()
				w.WriteHeader(http.StatusNotFound)
				return
			}
			ts.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	case http.MethodDelete:
		ts.mu.Lock()
		ts.closed[channel] = true
		ts.cond.Broadcast()
		ts.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}
