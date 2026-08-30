# gocache

`gocache` provides small, reusable cache primitives for Go applications.

| Package | Storage model | Expiration | Best fit |
|---|---|---|---|
| `rcu` | Immutable whole-map snapshots with lock-free reads | Refresh replaces the snapshot | Read-heavy projections rebuilt as a whole |
| `arc` | Fixed-capacity Adaptive Replacement Cache | Per-entry TTL with optional jitter | Per-key workloads that benefit from recency/frequency-aware eviction |
| `simple` | Unbounded concurrent map | Fixed per-entry TTL | Small cache-aside datasets that do not need eviction |

## Release status

The published `v0.1.0` release contains only `rcu`:

```bash
go get github.com/assurrussa/gocache@v0.1.0
```

The `arc` and `simple` packages are currently unreleased and are available in
the repository checkout. Their future release is intentionally separate from
this implementation change.

Supported package paths in the current checkout:

```go
import (
	"github.com/assurrussa/gocache/arc"
	"github.com/assurrussa/gocache/rcu"
	"github.com/assurrussa/gocache/simple"
)
```

## RCU snapshot cache

The `rcu` package builds a complete map off the reader path and publishes it
with one atomic pointer swap. `Get` and `Len` do not acquire a mutex. Refreshes
are serialized, so loaders never overlap.

```go
cache, err := rcu.New(ctx, loadCounters,
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
- failed loads preserve the last successfully published snapshot;
- `Snapshot` and loader maps are shallow-copied;
- periodic refresh defaults to one hour and can be configured or disabled.

## ARC cache

The `arc` package wraps HashiCorp's thread-safe generic ARC implementation with
TTL, expiration jitter, metrics, and cache-aside loading.

```go
cache, err := arc.New[string, User](ctx, "users", 10_000,
	arc.WithTTL(5*time.Minute),
	arc.WithTTLJitter(time.Minute),
)
if err != nil {
	return err
}

user, err := cache.GetOrLoad(ctx, userID, func(ctx context.Context) (User, error) {
	return loadUser(ctx, userID)
})
```

`GetOrLoad` coalesces concurrent loads for the same comparable key.
`GetOrLoadMany` deduplicates missing keys and safely coalesces overlapping
single and batch calls per key. If a joined load omits a key, the caller retries
that still-missing key through its own batch loader; this can invoke the loader
in multiple rounds. Loader errors, panics, or cancellation do not publish
staged batch values. The default TTL is five minutes, cleanup runs every minute,
and expiration receives up to one minute of random jitter.

`Get`, `Peek`, and `Contains` never return expired entries. `Len`, `Keys`, and
`Values` report the physical ARC contents and may briefly include expired
entries until lookup or background cleanup. `Get` updates ARC state; `Peek`
and `Contains` do not.

## Simple TTL cache

The `simple` package is an unbounded concurrent map with fixed TTL and
cache-aside duplicate-load suppression.

```go
cache, err := simple.New[string, User](ctx, "users",
	simple.WithTTL(10*time.Minute),
)
if err != nil {
	return err
}

user, err := cache.GetOrLoad(ctx, userID, func(ctx context.Context) (User, error) {
	return loadUser(ctx, userID)
})
```

The default TTL is ten minutes and cleanup runs every minute. `Get` strictly
rejects expired values, while `Len`, `Keys`, and `Values` can include them until
lookup or the next cleanup pass. Use `arc` instead when the cache needs a hard
capacity bound.

## Lifecycle and metrics

The constructor context for all packages controls background work. Canceling
it closes `Done`; cached reads, writes, and explicit load/refresh operations
remain available afterward.

`arc.WithMetrics` and `simple.WithMetrics` accept the narrow interface:

```go
type Metrics interface {
	Increment(key string)
	Gauge(key string, value any)
}
```

Implementations must be concurrency-safe and return promptly. Metric names are
`cache.<name>.hit`, `miss`, `expired`, `expired_get`, `delete` where supported,
and `len`.

## Dependency

Only `arc` adds a runtime dependency:
`github.com/hashicorp/golang-lru/arc/v2@v2.0.7`. The `rcu` and `simple`
implementations use the standard library; generic in-flight load coordination
is internal and does not use string-formatted keys.

## Verification

```bash
make check
make bench
```

`make check` verifies module tidiness, formatting, `go vet`, unit tests, the
race detector, and coverage.

## License

MIT
