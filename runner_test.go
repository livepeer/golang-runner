package liverunner

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRunnerRejectsUnknownModeBeforeRegistration(t *testing.T) {
	_, err := New(Options{
		OrchestratorURL: "http://127.0.0.1",
		BootstrapSecret: "bootstrap",
		RunnerURL:       "http://runner",
		App:             "app",
		Mode:            "single_shot",
	})
	if err == nil {
		t.Fatal("expected unknown mode error")
	}
}

func TestRunnerRegistrationHeartbeatO2RAndUnregister(t *testing.T) {
	ctx := context.Background()
	trickle := newTestTrickleServer(t)
	defer trickle.Close()

	var mu sync.Mutex
	var heartbeatAuth []string
	var payloads []heartbeatRequest
	var unregistered bool
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/runners/heartbeat":
			var payload heartbeatRequest
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode payload: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			heartbeatAuth = append(heartbeatAuth, r.Header.Get("Authorization"))
			payloads = append(payloads, payload)
			mu.Unlock()
			json.NewEncoder(w).Encode(heartbeatResponse{
				RunnerID:          "runner-1",
				Orchestrator:      "http://" + r.Host + "/api",
				HeartbeatInterval: "50ms",
				HeartbeatTTL:      "250ms",
				HeartbeatSecret:   "heartbeat-secret",
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
		OrchestratorURL:   orch.URL + "/api",
		BootstrapSecret:   "bootstrap-secret",
		RunnerURL:         "http://runner.example/app",
		App:               "livepeer-sample/echo",
		Mode:              ModePersistent,
		Capacity:          2,
		Status:            "ready",
		Label:             "echo-a",
		Version:           "v1",
		PriceInfo:         PriceInfo{PricePerUnit: 12, PixelsPerUnit: 34, Unit: "USD"},
		GPU:               &GPU{ID: "gpu-1", Name: "NVIDIA", VRAMMB: 24576},
		UnregisterOnClose: true,
		OnReserve: func(ev SessionEvent) {
			reserved = append(reserved, ev.SessionID)
		},
		OnRelease: func(ev SessionEvent) {
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
	if err := pub.Write(map[string]any{"event": "reserved", "session": "session-b"}); err != nil {
		t.Fatal(err)
	}
	if err := pub.Write(map[string]any{"event": "reserved", "session": "session-a"}); err != nil {
		t.Fatal(err)
	}
	if err := pub.Write(map[string]any{"event": "reserved", "session": "session-b"}); err != nil {
		t.Fatal(err)
	}
	if err := pub.Write(map[string]any{"event": "released", "session": "session-b"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return reflect.DeepEqual(reg.ActiveSessionIDs(), []string{"session-a"})
	})
	if !reflect.DeepEqual(reserved, []string{"session-b", "session-a", "session-b"}) {
		t.Fatalf("reserved callbacks = %#v", reserved)
	}
	if !reflect.DeepEqual(released, []string{"session-b"}) {
		t.Fatalf("released callbacks = %#v", released)
	}

	if err := reg.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !unregistered {
		t.Fatal("expected unregister")
	}
	if heartbeatAuth[0] != "bootstrap-secret" {
		t.Fatalf("initial heartbeat auth = %#v", heartbeatAuth)
	}
	if len(payloads) == 0 || payloads[0].RunnerURL != "http://runner.example/app" || payloads[0].App != "livepeer-sample/echo" {
		t.Fatalf("bad heartbeat payload: %#v", payloads)
	}
	if payloads[0].PriceInfo.PricePerUnit != 12 || payloads[0].GPU.Name != "NVIDIA" {
		t.Fatalf("missing price/gpu payload: %#v", payloads[0])
	}
}

func TestHeartbeat401RetriesBootstrapSecret(t *testing.T) {
	ctx := context.Background()
	var calls int
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			if got := r.Header.Get("Authorization"); got != "heartbeat-secret" {
				t.Fatalf("first auth = %q", got)
			}
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if got := r.Header.Get("Authorization"); got != "bootstrap-secret" {
			t.Fatalf("retry auth = %q", got)
		}
		json.NewEncoder(w).Encode(heartbeatResponse{
			RunnerID:          "runner-1",
			HeartbeatInterval: "5s",
			HeartbeatSecret:   "fresh-secret",
		})
	}))
	defer orch.Close()
	r, err := New(Options{
		OrchestratorURL:          orch.URL,
		BootstrapSecret:          "bootstrap-secret",
		RunnerURL:                "http://runner",
		App:                      "app",
		Mode:                     ModePersistent,
		DisableUnregisterOnClose: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	r.heartbeatSecret = "heartbeat-secret"
	if err := r.sendHeartbeat(ctx); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d", calls)
	}
}

func TestHeadersChannelsAndStopHelpers(t *testing.T) {
	ctx := context.Background()
	var seen []string
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path+" token="+r.Header.Get(HeaderSessionToken))
		switch {
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
	req.Header.Set(HeaderSessionID, "session-1")
	req.Header.Set(HeaderSessionToken, "token-1")
	req.Header.Set(HeaderSessionControl, orch.URL+"/runner/runner-1/session/session-1")
	req.Header.Set(HeaderRunnerRoute, "runner-1")
	if SessionToken(req) != "token-1" || SessionControl(req) != orch.URL+"/runner/runner-1/session/session-1" {
		t.Fatal("header helpers returned unexpected values")
	}
	channels, err := CreateTrickleChannels(ctx, nil, "", "", req, []ChannelRequest{{Name: "events", MimeType: "application/jsonl"}})
	if err != nil {
		t.Fatalf("create channels: %v", err)
	}
	if channels[0].InternalURL == "" || channels[0].ChannelName != "chan-events" {
		t.Fatalf("channels = %#v", channels)
	}
	deleted, err := DeleteTrickleChannels(ctx, nil, "", "", req, []string{"events"})
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
		"POST /runner/runner-1/session/session-1/channels token=token-1",
		"DELETE /runner/runner-1/session/session-1/channels token=token-1",
		"POST /runner/runner-1/session/session-1/stop token=token-1",
	}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("seen = %#v", seen)
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
