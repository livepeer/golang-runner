package liverunner

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type trackedResponseBody struct {
	io.Reader
	closed atomic.Int32
}

func (b *trackedResponseBody) Close() error {
	b.closed.Add(1)
	return nil
}

func TestSubscriberRefreshDiscardsCanceledAttempts(t *testing.T) {
	for _, status := range []int{0, http.StatusOK, 470} {
		for _, standardClient := range []bool{false, true} {
			name := strconv.Itoa(status) + "/HTTPDoer"
			if standardClient {
				name = strconv.Itoa(status) + "/http.Client"
			}
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var calls atomic.Int32
					stale := &trackedResponseBody{Reader: strings.NewReader("stale")}
					doer := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
						if calls.Add(1) == 1 {
							<-req.Context().Done()
							if status == 0 {
								return nil, req.Context().Err()
							}
							return &http.Response{StatusCode: status, Header: make(http.Header), Body: stale}, nil
						}
						// Let the superseded attempt finish before the replacement succeeds.
						time.Sleep(time.Nanosecond)
						return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("fresh"))}, nil
					})
					var client HTTPDoer = doer
					if standardClient {
						client = &http.Client{Transport: doer}
					}
					subscriber, err := NewTrickleSubscriber(TrickleSubscriberConfig{URL: "http://channel.example", Ctx: ctx, HTTPClient: client})
					if err != nil {
						t.Fatal(err)
					}
					start := time.Now()
					resp, err := subscriber.preconnect()
					if err != nil {
						t.Fatal(err)
					}
					body, err := io.ReadAll(resp.Body)
					resp.Body.Close()
					synctest.Wait()
					if err != nil || string(body) != "fresh" || calls.Load() != 2 || ctx.Err() != nil || time.Since(start) < preconnectRefreshTimeout {
						t.Fatalf("refresh result: body=%q, err=%v, calls=%d, context=%v", body, err, calls.Load(), ctx.Err())
					}
					if status != 0 && stale.closed.Load() != 1 {
						t.Fatalf("discarded response closed %d times, want 1", stale.closed.Load())
					}
				})
			})
		}
	}
}

func TestSubscriberCancellationDiscardsLateResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		body := &trackedResponseBody{Reader: strings.NewReader("late")}
		client := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
		})
		subscriber, err := NewTrickleSubscriber(TrickleSubscriberConfig{URL: "http://channel.example", Ctx: ctx, HTTPClient: client})
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			resp, err := subscriber.preconnect()
			if resp != nil {
				resp.Body.Close()
				t.Error("accepted a response after cancellation")
			}
			done <- err
		}()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
		synctest.Wait()
		if body.closed.Load() != 1 {
			t.Fatalf("late response closed %d times, want 1", body.closed.Load())
		}
	})
}

func TestSubscriberRejectsInvalidResponses(t *testing.T) {
	for _, tt := range []struct {
		name string
		resp *http.Response
	}{
		{"nil response", nil},
		{"200 without body", &http.Response{StatusCode: http.StatusOK}},
		{"470 without body", &http.Response{StatusCode: 470}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := testHTTPDoer(func(*http.Request) (*http.Response, error) { return tt.resp, nil })
			subscriber, err := NewTrickleSubscriber(TrickleSubscriberConfig{URL: "http://channel.example", HTTPClient: client})
			if err != nil {
				t.Fatal(err)
			}
			if resp, err := subscriber.preconnect(); err == nil || resp != nil {
				t.Fatalf("malformed client response: resp=%v, err=%v", resp, err)
			}
		})
	}
}

func TestSubscriberSetSeqClosesPrefetch(t *testing.T) {
	for _, status := range []int{http.StatusOK, 470} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				first := &trackedResponseBody{Reader: strings.NewReader("first")}
				prefetched := &trackedResponseBody{Reader: strings.NewReader("prefetch")}
				client := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
					header := make(http.Header)
					if req.URL.Path == "/0" {
						header.Set("Lp-Trickle-Seq", "0")
						return &http.Response{StatusCode: http.StatusOK, Header: header, Body: first}, nil
					}
					if req.URL.Path != "/1" {
						t.Errorf("unexpected prefetch: %s", req.URL.Path)
					}
					return &http.Response{StatusCode: status, Header: header, Body: prefetched}, nil
				})
				start := SequenceStart(0)
				subscriber, err := NewTrickleSubscriber(TrickleSubscriberConfig{URL: "http://channel.example", Ctx: ctx, Start: &start, HTTPClient: client})
				if err != nil {
					t.Fatal(err)
				}
				resp, err := subscriber.Read()
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				synctest.Wait()
				wantBefore := int32(0)
				if status != http.StatusOK {
					wantBefore = 1
				}
				if prefetched.closed.Load() != wantBefore {
					t.Fatalf("prefetch closed %d times before reset, want %d", prefetched.closed.Load(), wantBefore)
				}
				subscriber.SetSeq(42)
				subscriber.SetSeq(43)
				if first.closed.Load() != 1 || prefetched.closed.Load() != 1 {
					t.Fatalf("body close counts after reset: first=%d, prefetched=%d", first.closed.Load(), prefetched.closed.Load())
				}
			})
		})
	}
}

func TestSubscriberDiscardsQueuedPrefetchAfterSetSeq(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 100 {
			ctx, cancel := context.WithCancel(context.Background())
			var mu sync.Mutex
			var paths []string
			var bodies []*trackedResponseBody
			client := testHTTPDoer(func(req *http.Request) (*http.Response, error) {
				body := &trackedResponseBody{Reader: strings.NewReader(req.URL.Path)}
				mu.Lock()
				paths = append(paths, req.URL.Path)
				bodies = append(bodies, body)
				mu.Unlock()
				header := make(http.Header)
				header.Set("Lp-Trickle-Seq", strings.TrimPrefix(req.URL.Path, "/"))
				return &http.Response{StatusCode: http.StatusOK, Header: header, Body: body}, nil
			})
			start := SequenceStart(0)
			subscriber, err := NewTrickleSubscriber(TrickleSubscriberConfig{URL: "http://channel.example", Ctx: ctx, Start: &start, HTTPClient: client})
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			resp, err := subscriber.Read()
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			resp.Body.Close()
			subscriber.SetSeq(42)
			resp, err = subscriber.Read()
			if err != nil {
				cancel()
				t.Fatal(err)
			}
			resp.Body.Close()
			synctest.Wait()
			subscriber.SetSeq(44)
			cancel()
			synctest.Wait()
			mu.Lock()
			prefetches := 0
			for i, path := range paths {
				if path == "/43" {
					prefetches++
				}
				if bodies[i].closed.Load() != 1 {
					t.Errorf("response %s closed %d times, requests %v", path, bodies[i].closed.Load(), paths)
				}
			}
			if prefetches != 1 {
				t.Errorf("prefetched /43 %d times, requests %v", prefetches, paths)
			}
			mu.Unlock()
		}
	})
}
