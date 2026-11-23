# Livepeer Go runner SDK

This module provides Go packages for Livepeer runners and the Trickle protocol.
Use Go 1.25 or newer. See [examples](examples/) for runnable examples.

If the reservation event reader ends, the next successful heartbeat that returns
an O2R channel reconnects it. For the same channel URL, the reader requests the
next unread segment. Expired segments may be skipped. For a new URL, the reader
starts from the beginning. If a heartbeat returns 401, the runner retries with
its bootstrap credential.

## Development checks

Run the same checks that GitHub Actions runs on pull requests and on `main`.

```sh
test -z "$(gofmt -l .)"
go test -race -count=1 ./...
go vet ./...
go build ./...
```
