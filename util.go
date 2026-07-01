package liverunner

import (
	"errors"
	"net/url"
	"strings"
	"time"
)

func normalizeBase(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("orchestrator URL is required")
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return "", errors.New("invalid orchestrator URL")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	u.Fragment = ""
	return u.String(), nil
}

func joinEndpoint(base, suffix string) (string, error) {
	base, err := normalizeBase(base)
	if err != nil {
		return "", err
	}
	u, _ := url.Parse(base)
	if !strings.HasPrefix(suffix, "/") {
		suffix = "/" + suffix
	}
	u.Path = strings.TrimRight(u.Path, "/") + suffix
	u.Fragment = ""
	return u.String(), nil
}

func normalizeMode(mode string) (string, error) {
	mode = strings.TrimSpace(mode)
	if mode == "" {
		mode = ModePersistent
	}
	if mode != ModePersistent && mode != ModeSingleShot {
		return "", errors.New("live runner mode must be persistent or single-shot")
	}
	return mode, nil
}

func parseGoDuration(value string, fallback time.Duration) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	if d, err := time.ParseDuration(value); err == nil {
		return d
	}
	return fallback
}

func copyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, len(values))
	copy(out, values)
	return out
}
