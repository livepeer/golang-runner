package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	liverunner "github.com/livepeer/golang-runner"
)

type config struct {
	listenAddr      string
	orchestratorURL string
	bootstrapSecret string
	runnerURL       string
	app             string
	mode            string
	capacity        int
	label           string
	register        bool
	command         []string
}

type commandHandler struct {
	command []string
	env     []string
	stderr  io.Writer
	logger  *slog.Logger
}

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "configuration error: %v\n", err)
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger); err != nil {
		logger.Error("stdin/stdout runner stopped", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config, logger *slog.Logger) error {
	handler := commandHandler{
		command: cfg.command,
		stderr:  os.Stderr,
		logger:  logger,
	}
	server := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	listener, err := net.Listen("tcp", cfg.listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.listenAddr, err)
	}

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("stdin/stdout runner listening", "addr", listener.Addr().String(), "command", strings.Join(cfg.command, " "))
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()

	var runner *liverunner.Runner
	if cfg.register {
		runner, err = liverunner.Register(ctx, liverunner.Options{
			OrchestratorURL: cfg.orchestratorURL,
			BootstrapSecret: cfg.bootstrapSecret,
			RunnerURL:       cfg.runnerURL,
			App:             cfg.app,
			Mode:            cfg.mode,
			Capacity:        cfg.capacity,
			Label:           cfg.label,
		})
		if err != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
			return fmt.Errorf("register runner: %w", err)
		}
		logger.Info("registered live runner", "runner_id", runner.RunnerID())
		defer func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := runner.Close(closeCtx); err != nil {
				logger.Error("runner unregister failed", "err", err)
			}
		}()
	}

	select {
	case err := <-serverErr:
		if err != nil {
			return fmt.Errorf("serve http: %w", err)
		}
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown failed", "err", err)
	}
	return nil
}

func (h commandHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed\n", http.StatusMethodNotAllowed)
		return
	}
	if len(h.command) == 0 || strings.TrimSpace(h.command[0]) == "" {
		http.Error(w, "runner command is not configured\n", http.StatusInternalServerError)
		return
	}

	cmd := exec.CommandContext(r.Context(), h.command[0], h.command[1:]...)
	if len(h.env) > 0 {
		cmd.Env = h.env
	}
	cmd.Stdin = r.Body
	if h.stderr != nil {
		cmd.Stderr = h.stderr
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		http.Error(w, "failed to prepare runner command\n", http.StatusInternalServerError)
		h.warn("failed to prepare runner command", "err", err)
		return
	}
	if err := cmd.Start(); err != nil {
		http.Error(w, "failed to start runner command\n", http.StatusInternalServerError)
		h.warn("failed to start runner command", "command", strings.Join(h.command, " "), "err", err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}

	if _, err := io.Copy(flushWriter{w: w, flusher: flusher}, stdout); err != nil {
		h.warn("failed streaming runner stdout", "command", strings.Join(h.command, " "), "err", err)
	}
	if err := cmd.Wait(); err != nil {
		h.warn("runner command exited with error", "command", strings.Join(h.command, " "), "err", err)
	}
}

func (h commandHandler) warn(message string, attrs ...any) {
	if h.logger != nil {
		h.logger.Warn(message, attrs...)
	}
}

type flushWriter struct {
	w       io.Writer
	flusher http.Flusher
}

func (w flushWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	if n > 0 && w.flusher != nil {
		w.flusher.Flush()
	}
	return n, err
}

func parseConfig(args []string) (config, error) {
	capacity, err := envInt("RUNNER_CAPACITY", 1)
	if err != nil {
		return config{}, err
	}
	register, err := envBool("RUNNER_REGISTER", true)
	if err != nil {
		return config{}, err
	}

	cfg := config{
		listenAddr:      envString("RUNNER_LISTEN_ADDR", ":8080"),
		orchestratorURL: envString("LIVEPEER_ORCHESTRATOR_URL", ""),
		bootstrapSecret: envString("LIVEPEER_BOOTSTRAP_SECRET", ""),
		runnerURL:       envString("RUNNER_URL", ""),
		app:             envString("RUNNER_APP", "livepeer-sample/stdin-stdout"),
		mode:            envString("RUNNER_MODE", liverunner.ModePersistent),
		capacity:        capacity,
		label:           envString("RUNNER_LABEL", ""),
		register:        register,
	}

	fs := flag.NewFlagSet("stdin-stdout-runner", flag.ContinueOnError)
	fs.StringVar(&cfg.listenAddr, "listen", cfg.listenAddr, "HTTP listen address, or RUNNER_LISTEN_ADDR")
	fs.StringVar(&cfg.orchestratorURL, "orchestrator-url", cfg.orchestratorURL, "Livepeer orchestrator URL, or LIVEPEER_ORCHESTRATOR_URL")
	fs.StringVar(&cfg.bootstrapSecret, "bootstrap-secret", cfg.bootstrapSecret, "Livepeer bootstrap secret, or LIVEPEER_BOOTSTRAP_SECRET")
	fs.StringVar(&cfg.runnerURL, "runner-url", cfg.runnerURL, "externally reachable runner URL, or RUNNER_URL")
	fs.StringVar(&cfg.app, "app", cfg.app, "runner app name, or RUNNER_APP")
	fs.StringVar(&cfg.mode, "mode", cfg.mode, "runner mode, or RUNNER_MODE")
	fs.IntVar(&cfg.capacity, "capacity", cfg.capacity, "runner capacity, or RUNNER_CAPACITY")
	fs.StringVar(&cfg.label, "label", cfg.label, "runner label, or RUNNER_LABEL")
	fs.BoolVar(&cfg.register, "register", cfg.register, "register with the Livepeer orchestrator, or RUNNER_REGISTER")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}

	cfg.command = fs.Args()
	if len(cfg.command) == 0 {
		cfg.command = []string{"cat"}
	}

	if cfg.capacity <= 0 {
		return config{}, errors.New("capacity must be greater than zero")
	}
	if cfg.register {
		var missing []string
		if strings.TrimSpace(cfg.orchestratorURL) == "" {
			missing = append(missing, "-orchestrator-url or LIVEPEER_ORCHESTRATOR_URL")
		}
		if strings.TrimSpace(cfg.bootstrapSecret) == "" {
			missing = append(missing, "-bootstrap-secret or LIVEPEER_BOOTSTRAP_SECRET")
		}
		if strings.TrimSpace(cfg.runnerURL) == "" {
			missing = append(missing, "-runner-url or RUNNER_URL")
		}
		if len(missing) > 0 {
			return config{}, fmt.Errorf("registration is enabled but missing %s", strings.Join(missing, ", "))
		}
	}
	return cfg, nil
}

func envString(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) (int, error) {
	value, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return parsed, nil
}

func envBool(name string, fallback bool) (bool, error) {
	value, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean: %w", name, err)
	}
	return parsed, nil
}
