package liverunner

import "fmt"

type HTTPError struct {
	StatusCode int
	URL        string
	Body       string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("live runner HTTP error: status=%d url=%s body=%q", e.StatusCode, e.URL, e.Body)
}
