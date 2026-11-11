package liverunner

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// The trust set is fixed at construction, so responses cannot expand it.
type originPolicy map[string]bool

func newOriginPolicy(base string, allowed []string) (originPolicy, error) {
	u, err := parseOriginURL(base)
	if err != nil {
		return nil, fmt.Errorf("orchestrator URL: %w", err)
	}
	p := originPolicy{httpOrigin(u): true}
	for _, raw := range allowed {
		u, err := parseOriginURL(raw)
		if err != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery {
			return nil, errors.New("AllowedOrigins entries must be HTTP(S) origins without paths, queries, credentials, or fragments")
		}
		p[httpOrigin(u)] = true
	}
	return p, nil
}

func parseOriginURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || strings.Contains(raw, "#") {
		return nil, errors.New("URL must be absolute HTTP(S) without credentials or fragments")
	}
	for _, c := range u.Hostname() {
		if c > 127 {
			return nil, errors.New("URL hostname must be ASCII; use punycode for internationalized names")
		}
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, errors.New("invalid URL port")
		}
	}
	return u, nil
}

func httpOrigin(u *url.URL) string {
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == "https" {
			port = "443"
		}
	} else {
		n, _ := strconv.Atoi(port)
		port = strconv.Itoa(n)
	}
	host, zone, scoped := strings.Cut(u.Hostname(), "%")
	host = strings.ToLower(host)
	if scoped {
		host += "%" + zone
	}
	return u.Scheme + "://" + net.JoinHostPort(host, port)
}

func (p originPolicy) validate(raw string) error {
	u, err := parseOriginURL(raw)
	if err != nil {
		return err
	}
	if !p[httpOrigin(u)] {
		return errors.New("URL origin is not allowed")
	}
	return nil
}

// Check the fields used for routing as well as their potentially lossy serialization.
func (p originPolicy) validateURL(u *url.URL) error {
	if !p[httpOrigin(u)] {
		return errors.New("URL origin is not allowed")
	}
	return p.validate(u.String())
}

func (p originPolicy) validateRequest(req *http.Request) error {
	if req == nil || req.URL == nil {
		return errors.New("request URL is required")
	}
	path := req.URL.EscapedPath()
	if req.URL.Opaque != "" || (path != "" && !strings.HasPrefix(path, "/")) {
		return errors.New("opaque or non-absolute request targets are not allowed")
	}
	if err := p.validateURL(req.URL); err != nil {
		return err
	}
	if req.Host != "" {
		authority := &url.URL{Scheme: req.URL.Scheme, Host: req.Host}
		return p.validateURL(authority)
	}
	return nil
}

func (p originPolicy) guardClient(doer HTTPDoer) HTTPDoer {
	if doer == nil {
		doer = http.DefaultClient
	}
	if client, ok := doer.(*http.Client); ok {
		doer = p.guardHTTPClient(client)
	}
	return originHTTPDoer{client: doer, origins: p}
}

type originHTTPDoer struct {
	client  HTTPDoer
	origins originPolicy
}

func (c originHTTPDoer) Do(req *http.Request) (*http.Response, error) {
	if err := c.origins.validateRequest(req); err != nil {
		return nil, err
	}
	return c.client.Do(req)
}

func (p originPolicy) guardHTTPClient(client *http.Client) *http.Client {
	guarded := *client
	checkRedirect := client.CheckRedirect
	guarded.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if err := p.validateRequest(req); err != nil {
			return err
		}
		if checkRedirect != nil {
			if err := checkRedirect(req, via); err != nil {
				return err
			}
		} else if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return p.validateRequest(req)
	}
	return &guarded
}
