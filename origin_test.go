package liverunner

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOriginPolicy(t *testing.T) {
	policy, err := newOriginPolicy("https://Orch.Example/api", []string{"http://trickle.example:8080", "https://[::1]:8443", "http://[fe80::1%25eth0]"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		url     string
		allowed bool
	}{
		{"https://orch.example/other/path", true},
		{"https://ORCH.EXAMPLE:443/o2r", true},
		{"http://trickle.example:8080/o2r", true},
		{"http://trİckle.example:8080/o2r", false},
		{"https://[::1]:8443/o2r", true},
		{"http://[FE80::1%25eth0]/o2r", true},
		{"http://[fe80::1%25ETH0]/o2r", false},
		{"https://orch.example.evil/o2r", false},
		{"https://orch.example:444/o2r", false},
		{"http://orch.example/o2r", false},
		{"https://trickle.example:8080/o2r", false},
		{"https://user:pass@orch.example/o2r", false},
		{"https://orch.example/o2r#fragment", false},
		{"https://orch.example/o2r#", false},
		{"https://orch.example:99999/o2r", false},
		{"https://orch.example:bad/o2r", false},
		{"file:///o2r", false},
		{"/o2r", false},
		{"//orch.example/o2r", false},
		{"", false},
	} {
		t.Run(tt.url, func(t *testing.T) {
			if err := policy.validate(tt.url); (err == nil) != tt.allowed {
				t.Fatalf("validate(%q) = %v, allowed = %v", tt.url, err, tt.allowed)
			}
		})
	}
	for _, origin := range []string{"", "other.example", "/relative", "https://", "file:///bad", "https://user@other.example", "https://other.example/path", "https://other.example?query", "https://other.example#fragment", "https://other.example:99999", "https://lİvepeer.example"} {
		t.Run("configuration/"+origin, func(t *testing.T) {
			_, err := newOriginPolicy("https://orch.example/api", []string{origin})
			if err == nil {
				t.Fatal("newOriginPolicy accepted an invalid allowed origin")
			}
		})
	}
}

func TestRunnerRejectsUnicodeOrigin(t *testing.T) {
	runner, err := New(Options{OrchestratorURL: "http://livepeer.example", BootstrapSecret: "bootstrap", Apps: []AppOptions{{ID: "echo", App: "echo", RunnerURL: "http://runner.example"}}})
	if err != nil {
		t.Fatal(err)
	}
	// Unicode case folding would equate this hostname with livepeer.example,
	// while Go's transport resolves it to xn--livepeer-p0e.example via IDNA.
	malicious := "http://lİvepeer.example"
	resp := HeartbeatResponse{RunnerID: "owner", HeartbeatSecret: "secret", Orchestrator: malicious, HeartbeatTTL: "30s", Apps: []HeartbeatAppStatus{{ID: "echo", Status: "ready", SessionIDs: []string{}}}}
	if err := runner.applyHeartbeatResponse(&resp, runner.payloadLocked(), runner.apps, "bootstrap"); err == nil {
		t.Fatal("accepted a returned Unicode orchestrator hostname")
	}
	if runner.OrchestratorURL() != "http://livepeer.example" || runner.RunnerID() != "" {
		t.Fatal("rejected Unicode URL changed runner state")
	}
	req := httptest.NewRequest(http.MethodGet, malicious, nil)
	if _, err := runner.client.Do(req); err == nil {
		t.Fatal("accepted a request to a Unicode hostname")
	}
	if err := runner.origins.guardHTTPClient(http.DefaultClient).CheckRedirect(req, nil); err == nil {
		t.Fatal("accepted a redirect to a Unicode hostname")
	}
}

func TestRunnerAllowedOrchestratorMigration(t *testing.T) {
	ctx := context.Background()
	const untrustedURL = "https://untrusted.example"
	response := func() HeartbeatResponse {
		return HeartbeatResponse{
			RunnerID: "owner", HeartbeatTTL: "1m",
			Apps: []HeartbeatAppStatus{
				{ID: "ready", Status: "ready", SessionIDs: []string{}},
				{ID: "not-ready", Status: "not-ready", SessionIDs: []string{}},
				{ID: "retained", Status: "draining", SessionIDs: []string{"session"}},
			},
		}
	}
	trusted := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/runners/heartbeat" || req.Header.Get("Authorization") != "secret" {
			t.Errorf("migrated request = %s, auth = %q", req.URL.Path, req.Header.Get("Authorization"))
		}
		resp := response()
		resp.Orchestrator = untrustedURL
		resp.HeartbeatSecret = "untrusted-secret"
		json.NewEncoder(w).Encode(resp)
	}))
	defer trusted.Close()
	original := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		resp := response()
		resp.Orchestrator = trusted.URL + "/api"
		resp.HeartbeatSecret = "secret"
		json.NewEncoder(w).Encode(resp)
	}))
	defer original.Close()
	allowed := []string{trusted.URL}
	runner, err := New(Options{
		OrchestratorURL: original.URL, BootstrapSecret: "bootstrap", AllowedOrigins: allowed,
		DisableUnregisterOnClose: true,
		Apps:                     []AppOptions{{App: "echo", RunnerURL: "https://runner.example/ready"}, {App: "echo", RunnerURL: "https://runner.example/not-ready"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close(ctx)
	allowed[0] = untrustedURL // Caller mutation cannot expand the trust set.
	if err := runner.sendHeartbeat(ctx); err != nil {
		t.Fatal(err)
	}
	if runner.OrchestratorURL() != trusted.URL+"/api" || !reflect.DeepEqual(runner.AppIDs(), []string{"ready", "not-ready"}) {
		t.Fatal("trusted migration was not applied")
	}
	if runner.HeartbeatInterval() != defaultHeartbeatInterval {
		t.Fatal("omitted interval did not use the default")
	}
	if err := runner.sendHeartbeat(ctx); err == nil || !strings.Contains(err.Error(), "origin is not allowed") {
		t.Fatalf("untrusted second migration = %v", err)
	}
	if runner.OrchestratorURL() != trusted.URL+"/api" || runner.HeartbeatSecret() != "secret" || runner.HeartbeatTTL() != time.Minute {
		t.Fatal("untrusted migration changed accepted state")
	}
}

func TestRunnerBlocksCrossOriginRedirects(t *testing.T) {
	for _, kind := range []string{"heartbeat", "unregister", "o2r", "o2r after sequence reset"} {
		t.Run(kind, func(t *testing.T) {
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				t.Errorf("request reached an untrusted redirect target: %s", req.URL)
				w.WriteHeader(http.StatusNotFound)
			}))
			defer target.Close()
			orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if kind == "heartbeat" || req.URL.Path == "/runners/owner/unregister" || strings.HasPrefix(req.URL.Path, "/o2r/") {
					if kind == "o2r after sequence reset" && req.URL.Path == "/o2r/0" {
						w.Header().Set("Lp-Trickle-Seq", "0")
						w.Header().Set("Lp-Trickle-Latest", "4")
						w.WriteHeader(470)
						return
					}
					http.Redirect(w, req, target.URL+"/redirected", http.StatusTemporaryRedirect)
					return
				}
				resp := HeartbeatResponse{RunnerID: "owner", HeartbeatSecret: "secret", HeartbeatTTL: "30s", Apps: []HeartbeatAppStatus{{ID: "echo", Status: "ready", SessionIDs: []string{}}}}
				if strings.HasPrefix(kind, "o2r") {
					resp.O2R = &Channel{URL: "http://" + req.Host + "/o2r"}
				}
				json.NewEncoder(w).Encode(resp)
			}))
			defer orch.Close()
			runner, err := New(Options{OrchestratorURL: orch.URL, BootstrapSecret: "bootstrap", DisableUnregisterOnClose: kind != "unregister", Apps: []AppOptions{{ID: "echo", App: "echo", RunnerURL: "http://runner.example"}}})
			if err != nil {
				t.Fatal(err)
			}
			defer runner.Close(context.Background())
			err = runner.sendHeartbeat(context.Background())
			if kind == "heartbeat" {
				if err == nil || !strings.Contains(err.Error(), "origin is not allowed") {
					t.Fatalf("heartbeat redirect = %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if kind == "unregister" {
					if err := runner.Close(context.Background()); err == nil || !strings.Contains(err.Error(), "origin is not allowed") {
						t.Fatalf("unregister redirect = %v", err)
					}
				} else {
					done := make(chan struct{})
					go func() { runner.wg.Wait(); close(done) }()
					select {
					case <-done:
					case <-time.After(2 * time.Second):
						t.Fatal("O2R reader did not stop after rejected redirect")
					}
				}
			}
		})
	}
}

func TestRunnerFollowsTrustedRedirects(t *testing.T) {
	for _, separateOrigin := range []bool{false, true} {
		name := "same origin"
		if separateOrigin {
			name = "allowlisted origin"
		}
		t.Run(name, func(t *testing.T) {
			respond := func(w http.ResponseWriter, req *http.Request) {
				json.NewEncoder(w).Encode(HeartbeatResponse{RunnerID: "owner", HeartbeatSecret: "secret", HeartbeatTTL: "30s", Apps: []HeartbeatAppStatus{{ID: "echo", Status: "ready", SessionIDs: []string{}}}})
			}
			target := httptest.NewServer(http.HandlerFunc(respond))
			defer target.Close()
			orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/redirected" {
					respond(w, req)
					return
				}
				destination := "/redirected"
				if separateOrigin {
					destination = target.URL + destination
				}
				http.Redirect(w, req, destination, http.StatusTemporaryRedirect)
			}))
			defer orch.Close()
			redirects := 0
			opts := Options{
				OrchestratorURL: orch.URL, BootstrapSecret: "bootstrap", DisableUnregisterOnClose: true,
				Apps:       []AppOptions{{ID: "echo", App: "echo", RunnerURL: "http://runner.example"}},
				HTTPClient: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { redirects++; return nil }},
			}
			if separateOrigin {
				opts.AllowedOrigins = []string{target.URL}
			}
			runner, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer runner.Close(context.Background())
			if err := runner.sendHeartbeat(context.Background()); err != nil || redirects != 1 || runner.RunnerID() != "owner" {
				t.Fatalf("trusted redirect = %v, redirects = %d, runner ID = %q", err, redirects, runner.RunnerID())
			}
		})
	}
}

func TestOriginPolicyPreservesClientRedirectPolicy(t *testing.T) {
	policy, err := newOriginPolicy("https://orch.example", nil)
	if err != nil {
		t.Fatal(err)
	}
	denied := errors.New("custom redirect rejection")
	client := &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return denied }}
	guarded := policy.guardHTTPClient(client)
	req := httptest.NewRequest(http.MethodGet, "https://orch.example/redirect", nil)
	if err := guarded.CheckRedirect(req, nil); !errors.Is(err, denied) || guarded.Timeout != client.Timeout {
		t.Fatalf("custom client policy or configuration was lost: %v", err)
	}
	if guarded == client || !errors.Is(client.CheckRedirect(req, nil), denied) {
		t.Fatal("guarding modified the caller's client")
	}
	if err := policy.guardHTTPClient(&http.Client{}).CheckRedirect(req, make([]*http.Request, 10)); err == nil {
		t.Fatal("guarding removed the default redirect limit")
	}
}

type testHTTPDoer func(*http.Request) (*http.Response, error)

func (f testHTTPDoer) Do(req *http.Request) (*http.Response, error)        { return f(req) }
func (f testHTTPDoer) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestRunnerUsesHTTPClientForO2RAfterSequenceReset(t *testing.T) {
	for _, standardClient := range []bool{false, true} {
		name := "HTTPDoer"
		if standardClient {
			name = "http.Client"
		}
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			var requests []string
			allowedDials, deniedDials := 0, 0
			allow := true
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Connection", "close")
				mu.Lock()
				defer mu.Unlock()
				requests = append(requests, req.Method+" "+req.URL.Path)
				switch req.Method + " " + req.URL.Path {
				case "POST /runners/heartbeat":
					json.NewEncoder(w).Encode(HeartbeatResponse{
						RunnerID: "owner", HeartbeatSecret: "secret", HeartbeatTTL: "30s",
						Apps: []HeartbeatAppStatus{{ID: "echo", Status: "ready", SessionIDs: []string{}}},
						O2R:  &Channel{URL: "http://" + req.Host + "/o2r"},
					})
				case "GET /o2r/0":
					w.Header().Set("Lp-Trickle-Latest", "4")
					w.WriteHeader(470)
				case "GET /o2r/4":
					w.Header().Set("Lp-Trickle-Seq", "4")
					io.WriteString(w, `{"event":"reserved","app_id":"echo","session":"session"}`)
					allow = false
				default:
					t.Errorf("request bypassed the destination policy: %s", req.URL)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			transport := &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
				host, port, err := net.SplitHostPort(address)
				if err != nil {
					return nil, err
				}
				ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
				if err != nil {
					return nil, err
				}
				destination := net.JoinHostPort(ips[0].String(), port)
				mu.Lock()
				defer mu.Unlock()
				if !allow || destination != server.Listener.Addr().String() {
					deniedDials++
					return nil, errors.New("resolved destination denied")
				}
				allowedDials++
				return (&net.Dialer{}).DialContext(ctx, network, destination)
			}}
			defer transport.CloseIdleConnections()
			var client HTTPDoer = &http.Client{Transport: transport}
			if !standardClient {
				client = testHTTPDoer(client.Do)
			}
			reserved := make(chan SessionEvent, 2)
			runner, err := New(Options{
				OrchestratorURL: server.URL, BootstrapSecret: "bootstrap", HTTPClient: client,
				DisableUnregisterOnClose: true,
				Apps:                     []AppOptions{{ID: "echo", App: "echo", RunnerURL: "http://runner.example"}},
				OnReserve:                func(event SessionEvent) { reserved <- event },
			})
			if err != nil {
				t.Fatal(err)
			}
			defer runner.Close(context.Background())
			if err := runner.sendHeartbeat(context.Background()); err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() { runner.wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("O2R reader did not stop after destination rejection")
			}
			if len(reserved) != 1 || (<-reserved).SessionID != "session" {
				t.Fatal("O2R did not resume through the configured client")
			}
			mu.Lock()
			defer mu.Unlock()
			want := []string{"POST /runners/heartbeat", "GET /o2r/0", "GET /o2r/4"}
			if !reflect.DeepEqual(requests, want) || allowedDials != 3 || deniedDials == 0 {
				t.Fatalf("requests=%v, allowed dials=%d, denied dials=%d", requests, allowedDials, deniedDials)
			}
		})
	}
}

func TestOriginPolicyValidatesRequestRouting(t *testing.T) {
	policy, err := newOriginPolicy("http://trusted.example", []string{"http://[::1]:8443"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name    string
		mutate  func(*http.Request)
		allowed bool
	}{
		{"opaque target hides dial host", func(req *http.Request) {
			req.URL.Host = "untrusted.example"
			req.URL.Opaque = "//trusted.example/redirected"
		}, false},
		{"Host override", func(req *http.Request) { req.Host = "untrusted.example" }, false},
		{"absolute URI in path", func(req *http.Request) { req.URL.Path = "http://untrusted.example/redirected" }, false},
		{"encoded leading slash", func(req *http.Request) {
			req.URL.Path = "/@untrusted.example/redirected"
			req.URL.RawPath = "%2f@untrusted.example/redirected"
		}, false},
		{"prefixed bracket URL host", func(req *http.Request) {
			req.URL.Host = "untrusted.example[::1]:8443"
			req.Host = ""
		}, false},
		{"prefixed bracket Host override", func(req *http.Request) {
			req.URL.Host = "[::1]:8443"
			req.Host = "untrusted.example[::1]:8443"
		}, false},
		{"missing URL authority", func(req *http.Request) {
			req.URL = &url.URL{Scheme: "http", OmitHost: true, Path: "//trusted.example/redirected"}
			req.Host = ""
		}, false},
		{"uppercase scheme field", func(req *http.Request) { req.URL.Scheme = "HTTP" }, false},
		{"trusted IPv6 authority", func(req *http.Request) {
			req.URL.Host = "[::1]:8443"
			req.Host = "[::1]:8443"
		}, true},
		{"padded default port", func(req *http.Request) {
			req.URL.Host = "TRUSTED.EXAMPLE:0080"
			req.Host = "trusted.example:80"
		}, true},
		{"trusted Host override", func(req *http.Request) { req.Host = "TRUSTED.EXAMPLE:80" }, true},
	} {
		for _, redirect := range []bool{false, true} {
			name := tt.name + "/initial"
			if redirect {
				name = tt.name + "/redirect"
			}
			t.Run(name, func(t *testing.T) {
				calls := 0
				doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
					calls++
					resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody, Request: req}
					if redirect && calls == 1 {
						resp.StatusCode = http.StatusTemporaryRedirect
						resp.Header.Set("Location", "/redirected")
					}
					return resp, nil
				})
				var client HTTPDoer = doer
				if redirect {
					client = &http.Client{Transport: doer, CheckRedirect: func(req *http.Request, via []*http.Request) error {
						tt.mutate(req)
						return nil
					}}
				}
				req, err := http.NewRequest(http.MethodGet, "http://trusted.example/", nil)
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Authorization", "bootstrap-secret")
				if !redirect {
					tt.mutate(req)
				}
				resp, err := policy.guardClient(client).Do(req)
				if resp != nil {
					resp.Body.Close()
				}
				if (err == nil) != tt.allowed {
					t.Fatalf("request error = %v, allowed = %v", err, tt.allowed)
				}
				want := 0
				if redirect {
					want++
				}
				if tt.allowed {
					want++
				}
				if calls != want {
					t.Fatalf("transport calls = %d, want %d", calls, want)
				}
			})
		}
	}
}
