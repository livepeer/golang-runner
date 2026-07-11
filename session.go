package liverunner

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func SessionToken(r *http.Request) string {
	if r == nil {
		return ""
	}
	return r.Header.Get(HeaderSessionToken)
}

func SessionControl(r *http.Request) string {
	if r == nil {
		return ""
	}
	return r.Header.Get(HeaderSessionControl)
}

func sessionControlEndpoint(req *http.Request, suffix string) (string, error) {
	if req == nil {
		return "", errors.New("request is required")
	}
	controlURL := strings.TrimSpace(SessionControl(req))
	if controlURL == "" {
		return "", errors.New("session_control is required")
	}
	return joinEndpoint(controlURL, suffix)
}

func sessionEndpoint(orchestratorURL, runnerID string, req *http.Request, suffix string) (string, string, error) {
	if req == nil {
		return "", "", errors.New("request is required")
	}
	token := strings.TrimSpace(SessionToken(req))
	if token == "" {
		return "", "", errors.New("session_token is required")
	}
	if controlURL := strings.TrimSpace(SessionControl(req)); controlURL != "" {
		endpoint, err := joinEndpoint(controlURL, suffix)
		return endpoint, token, err
	}
	sessionID := strings.TrimSpace(req.Header.Get(HeaderSessionID))
	if sessionID == "" {
		return "", "", errors.New("session_id is required")
	}
	if runnerID == "" {
		runnerID = strings.TrimSpace(req.Header.Get(HeaderRunnerRoute))
	}
	if runnerID == "" {
		return "", "", errors.New("runner_id is required")
	}
	if !strings.HasPrefix(suffix, "/") {
		suffix = "/" + suffix
	}
	endpoint, err := joinEndpoint(orchestratorURL, "/runner/"+url.PathEscape(runnerID)+"/session/"+url.PathEscape(sessionID)+suffix)
	return endpoint, token, err
}

func StopSession(ctx context.Context, client HTTPDoer, req *http.Request, timeout time.Duration) error {
	endpoint, err := sessionControlEndpoint(req, "stop")
	if err != nil {
		return err
	}
	headers := map[string]string{}
	if token := strings.TrimSpace(SessionToken(req)); token != "" {
		headers[HeaderSessionToken] = token
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return postEmpty(ctx, client, endpoint, headers, timeout)
}
