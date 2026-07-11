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
RUNNER_LABEL=stdin-stdout-a
```
