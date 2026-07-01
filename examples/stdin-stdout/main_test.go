package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandHandlerStreamsPostBodyToStdout(t *testing.T) {
	handler := testHandler(helperCommand("echo"))

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("hello runner\n"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	resp := rec.Result()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %q", resp.StatusCode, string(body))
	}
	if got := resp.Header.Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("content type = %q", got)
	}
	if string(body) != "hello runner\n" {
		t.Fatalf("body = %q", string(body))
	}
}

func TestCommandHandlerRejectsNonPost(t *testing.T) {
	handler := testHandler(helperCommand("echo"))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	resp := rec.Result()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Allow"); got != http.MethodPost {
		t.Fatalf("allow = %q", got)
	}
}

func TestCommandHandlerReturns500WhenCommandCannotStart(t *testing.T) {
	handler := testHandler([]string{filepath.Join(t.TempDir(), "missing-command")})

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("input"))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	resp := rec.Result()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %q", resp.StatusCode, string(body))
	}
}

func testHandler(command []string) commandHandler {
	return commandHandler{
		command: command,
		env:     append(os.Environ(), "GO_WANT_STDIN_STDOUT_HELPER=1"),
		stderr:  io.Discard,
		logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func helperCommand(mode string) []string {
	return []string{os.Args[0], "-test.run=TestHelperProcess", "--", mode}
}

func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_STDIN_STDOUT_HELPER") != "1" {
		return
	}
	args := os.Args
	for i, arg := range args {
		if arg == "--" && i+1 < len(args) {
			switch args[i+1] {
			case "echo":
				_, _ = io.Copy(os.Stdout, os.Stdin)
				os.Exit(0)
			}
		}
	}
	os.Exit(2)
}
