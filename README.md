# Livepeer Go runner SDK

This module provides Go packages for Livepeer runners and the Trickle protocol.
Use Go 1.25 or newer. See [examples](examples/) for runnable examples.

## Development checks

Run the same checks that GitHub Actions runs on pull requests and on `main`.

```sh
test -z "$(gofmt -l .)"
go test -race -count=1 ./...
go vet ./...
go build ./...
```
