# Stdin/stdout runner example

This example hosts one HTTP endpoint for a Livepeer runner. Each `POST` request body is connected to the configured command's stdin, and the command's stdout is streamed back as the HTTP response.

The command is trusted startup configuration. Requests cannot choose or override it.

## Local demo

Run without Livepeer registration:

```sh
RUNNER_REGISTER=false go run ./examples/stdin-stdout
```

In another terminal, send a request:

```sh
printf 'hello runner\n' | curl -sS -X POST --data-binary @- http://127.0.0.1:8080
```

The default command is `cat`, so the response should match the request body.

To override the command at startup:

```sh
RUNNER_REGISTER=false go run ./examples/stdin-stdout -- tr a-z A-Z
printf 'hello runner\n' | curl -sS -X POST --data-binary @- http://127.0.0.1:8080
```

The response should be uppercased.

## Registered runner

Set the required registration values with environment variables or flags:

```sh
LIVEPEER_ORCHESTRATOR_URL=https://orchestrator.example \
LIVEPEER_BOOTSTRAP_SECRET=bootstrap-secret \
RUNNER_URL=https://runner.example \
go run ./examples/stdin-stdout
```

Equivalent flags are also available:

```sh
go run ./examples/stdin-stdout \
  -orchestrator-url https://orchestrator.example \
  -bootstrap-secret bootstrap-secret \
  -runner-url https://runner.example
```

Common optional settings:

```sh
RUNNER_LISTEN_ADDR=:8080
RUNNER_APP=livepeer-sample/stdin-stdout
RUNNER_MODE=persistent
RUNNER_CAPACITY=1
```

## Registering multiple apps

Register apps with independent URLs, versions, capacity, and pricing through one runner:

```go
runner, err := liverunner.Register(ctx, liverunner.Options{
	OrchestratorURL: "https://orchestrator.example",
	BootstrapSecret: "bootstrap-secret",
	Apps: []liverunner.AppOptions{
		{App: "example/echo", RunnerURL: "https://echo.example", Version: "v1", Capacity: 2},
		{App: "example/transform", RunnerURL: "https://transform.example", Mode: liverunner.ModeSingleShot},
	},
})
if err != nil {
	return err
}
defer runner.Close(ctx)
slog.Info("registered apps", "app_ids", runner.AppIDs())
```

`AppOptions.Version` is the app version; the SDK reports its own version separately.
The `OnReserve` and `OnRelease` callbacks are shared by all apps, and each event's
`AppID` identifies the app.

`AppIDs()` returns a copy of the IDs in configuration order. The runner retains
IDs assigned by the node; set `AppOptions.ID` to request a stable ID. Use the app
ID with `runner.ActiveSessionIDs(appID)` and
`runner.StopSession(ctx, appID, sessionID, sessionToken)`. Apps are configured at
startup, and closing the runner unregisters them by default.
