package liverunner

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

type createProxyRequest struct {
	TargetURL string `json:"target_url,omitempty"`
}

func (r *Runner) CreateProxy(ctx context.Context, req *http.Request, targetURL string) (Proxy, error) {
	return CreateProxy(ctx, r.client, r.OrchestratorURL(), r.RunnerID(), req, targetURL)
}

func CreateProxy(ctx context.Context, client HTTPDoer, orchestratorURL, runnerID string, req *http.Request, targetURL string) (Proxy, error) {
	targetURL = strings.TrimSpace(targetURL)
	endpoint, token, err := proxyEndpoint(orchestratorURL, runnerID, req)
	if err != nil {
		return Proxy{}, err
	}
	var resp Proxy
	if err := requestJSON(ctx, client, http.MethodPost, endpoint, createProxyRequest{TargetURL: targetURL}, map[string]string{HeaderSessionToken: token}, defaultTimeout, &resp); err != nil {
		return Proxy{}, err
	}
	if strings.TrimSpace(resp.ProxyID) == "" || strings.TrimSpace(resp.URL) == "" {
		return Proxy{}, errors.New("live runner proxy response missing proxy_id or url")
	}
	return resp, nil
}

func proxyEndpoint(orchestratorURL, runnerID string, req *http.Request) (string, string, error) {
	return sessionEndpoint(orchestratorURL, runnerID, req, "proxy")
}
