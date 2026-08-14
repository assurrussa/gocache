# gocache

`gocache` provides small, reusable cache primitives for Go applications. The
first supported package is an RCU-style whole-snapshot cache for read-heavy
workloads.

## Install

```bash
go get github.com/assurrussa/gocache@v0.1.0
```

Supported import:

```go
import "github.com/assurrussa/gocache/rcu"
```

## RCU snapshot cache

The `rcu` package builds a complete map off the reader path and publishes it
with one atomic pointer swap. `Get` and `Len` do not acquire a mutex. Refreshes
are serialized, so loaders never overlap.

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

cache, err := rcu.New(ctx, func(ctx context.Context) (map[string]int, error) {
    return loadCounters(ctx)
},
    rcu.WithRefreshInterval(time.Minute),
    rcu.WithErrorHandler(func(_ context.Context, err error) {
        slog.Error("refresh counters", "err", err)
    }),
)
if err != nil {
    return err
}
if err := cache.WaitInitial(ctx); err != nil {
    return err
}

value, found := cache.Get("active")
cache.Notify() // non-blocking, coalesced event refresh
```

Important semantics:

- construction starts one context-bound background goroutine;
- loaders should honor context cancellation and must not mutate returned
  snapshots after returning;
- the initial load is asynchronous and observable through `WaitInitial`;
- `Notify` is non-blocking and coalesces bursts while a refresh is running;
- periodic refresh defaults to one hour and can be configured or disabled;
- failed loads preserve the last successfully published snapshot;
- the loader's map and maps returned by `Snapshot` are shallow-copied;
- values containing pointers, slices, or maps must still be treated as
  immutable after publication;
- the optional error handler receives only background refresh failures;
  callers handle errors returned by synchronous `Refresh` themselves, and the
  handler should return promptly so it does not delay later event refreshes.

## Verification

```bash
make check
make bench
```

`make check` verifies module tidiness, formatting, `go vet`, unit tests, the
race detector, and coverage.

## License

MIT
