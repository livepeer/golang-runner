package liverunner

import (
	"context"
	"errors"
	"net/http"
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

func StopSession(ctx context.Context, client HTTPDoer, req *http.Request, timeout time.Duration) error {
	if req == nil {
		return errors.New("request is required")
	}
	controlURL := strings.TrimSpace(SessionControl(req))
	if controlURL == "" {
		return errors.New("session_control is required")
	}
	endpoint, err := joinEndpoint(controlURL, "stop")
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
