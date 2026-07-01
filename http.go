package liverunner

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

func requestJSON(ctx context.Context, c HTTPDoer, method, url string, payload any, headers map[string]string, timeout time.Duration, out any) error {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if c == nil {
		c = http.DefaultClient
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return &HTTPError{StatusCode: resp.StatusCode, URL: url, Body: string(data)}
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, out)
}

func postEmpty(ctx context.Context, c HTTPDoer, url string, headers map[string]string, timeout time.Duration) error {
	return requestJSON(ctx, c, http.MethodPost, url, nil, headers, timeout, nil)
}
