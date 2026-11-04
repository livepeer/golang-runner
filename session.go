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

func sessionEndpoint(orchestratorURL string, req *http.Request, suffix string) (string, string, error) {
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
	appID := strings.TrimSpace(req.Header.Get(HeaderRunnerRoute))
	if appID == "" {
		return "", "", errors.New("app_id is required")
	}
	if !strings.HasPrefix(suffix, "/") {
		suffix = "/" + suffix
	}
	endpoint, err := joinEndpoint(orchestratorURL, "/runner/"+url.PathEscape(appID)+"/session/"+url.PathEscape(sessionID)+suffix)
	return endpoint, token, err
}

func (r *Runner) StopSession(ctx context.Context, appID, sessionID, sessionToken string) error {
	appID = strings.TrimSpace(appID)
	if appID == "" {
		return errors.New("app_id is required")
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return errors.New("session_id is required")
	}
	sessionToken = strings.TrimSpace(sessionToken)
	if sessionToken == "" {
		return errors.New("session_token is required")
	}
	r.mu.Lock()
	base, client, timeout := r.orchestratorURL, r.client, r.timeout
	r.mu.Unlock()
	endpoint, err := url.JoinPath(base, "runner", url.PathEscape(appID), "session", url.PathEscape(sessionID), "stop")
	if err != nil {
		return err
	}
	return stopSession(ctx, client, endpoint, sessionToken, timeout)
}

func StopSession(ctx context.Context, client HTTPDoer, req *http.Request, timeout time.Duration) error {
	endpoint, err := sessionControlEndpoint(req, "stop")
	if err != nil {
		return err
	}
	return stopSession(ctx, client, endpoint, SessionToken(req), timeout)
}

func stopSession(ctx context.Context, client HTTPDoer, endpoint, token string, timeout time.Duration) error {
	headers := map[string]string{}
	if token = strings.TrimSpace(token); token != "" {
		headers[HeaderSessionToken] = token
	}
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return postEmpty(ctx, client, endpoint, headers, timeout)
}
