package rcu

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const notificationBuffer = 1

var (
	// ErrNilCache reports a method call that requires a constructed Cache.
	ErrNilCache = errors.New("rcu: cache is nil")
	// ErrNilContext reports a nil context passed to New, Refresh, or
	// WaitInitial.
	ErrNilContext = errors.New("rcu: context must not be nil")
	// ErrNilLoader reports a nil Loader passed to New.
	ErrNilLoader = errors.New("rcu: loader must not be nil")
)

// Loader builds one complete snapshot. Cache serializes calls to a Loader.
type Loader[K comparable, V any] func(context.Context) (map[K]V, error)

// Cache publishes immutable whole-map snapshots for lock-free readers.
// Construct a Cache with New; the zero value is not usable for refreshes.
type Cache[K comparable, V any] struct {
	loader          Loader[K, V]
	refreshInterval time.Duration
	errorHandler    ErrorHandler

	data      atomic.Pointer[map[K]V]
	refreshMu sync.Mutex
	notify    chan struct{}

	initialDone chan struct{}
	initialErr  error
	done        chan struct{}
}

// New constructs a Cache and starts its context-bound background worker. The
// first load is asynchronous; use WaitInitial when startup depends on it.
func New[K comparable, V any](
	ctx context.Context,
	loader Loader[K, V],
	options ...Option,
) (*Cache[K, V], error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if loader == nil {
		return nil, ErrNilLoader
	}
	cfg := config{refreshInterval: DefaultRefreshInterval}
	for _, option := range options {
		if option == nil {
			return nil, ErrNilOption
		}
		if err := option.apply(&cfg); err != nil {
			return nil, fmt.Errorf("rcu: apply option: %w", err)
		}
	}

	cache := &Cache[K, V]{
		loader:          loader,
		refreshInterval: cfg.refreshInterval,
		errorHandler:    cfg.errorHandler,
		notify:          make(chan struct{}, notificationBuffer),
		initialDone:     make(chan struct{}),
		done:            make(chan struct{}),
	}
	go cache.run(ctx)
	return cache, nil
}

// Get returns one value from the current snapshot without acquiring a lock.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	if c == nil {
		var zero V
		return zero, false
	}
	data := c.load()
	value, found := data[key]
	return value, found
}

// Len returns the number of entries in the current snapshot without acquiring
// a lock.
func (c *Cache[K, V]) Len() int {
	if c == nil {
		return 0
	}
	return len(c.load())
}

// Snapshot returns a shallow copy of the current snapshot. Mutating the
// returned map cannot change the cache. Values that contain reference types
// remain the caller's responsibility.
func (c *Cache[K, V]) Snapshot() map[K]V {
	if c == nil {
		return map[K]V{}
	}
	return cloneMap(c.load())
}

// Refresh synchronously builds and atomically publishes one complete
// snapshot. Concurrent calls are serialized. A failed refresh preserves the
// previously published snapshot.
func (c *Cache[K, V]) Refresh(ctx context.Context) error {
	if c == nil {
		return ErrNilCache
	}
	if ctx == nil {
		return ErrNilContext
	}

	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("rcu: refresh canceled before load: %w", err)
	}
	next, err := c.loader(ctx)
	if err != nil {
		return fmt.Errorf("rcu: load snapshot: %w", err)
	}
	snapshot := cloneMap(next)
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("rcu: refresh canceled before publish: %w", err)
	}
	c.data.Store(&snapshot)
	return nil
}

// Notify requests an event-driven refresh. It never blocks. Bursts are
// coalesced into at most one queued refresh while the worker is busy.
func (c *Cache[K, V]) Notify() {
	if c == nil {
		return
	}
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

// WaitInitial waits for the first background load attempt and returns its
// result. It is safe for multiple callers.
func (c *Cache[K, V]) WaitInitial(ctx context.Context) error {
	if c == nil {
		return ErrNilCache
	}
	if ctx == nil {
		return ErrNilContext
	}
	select {
	case <-c.initialDone:
		return c.initialErr
	default:
	}
	select {
	case <-c.initialDone:
		return c.initialErr
	case <-ctx.Done():
		return fmt.Errorf("rcu: wait for initial load: %w", ctx.Err())
	}
}

// Done is closed after the context passed to New stops the background worker.
// Reads and explicit Refresh calls remain usable after the worker stops.
func (c *Cache[K, V]) Done() <-chan struct{} {
	if c == nil {
		return nil
	}
	return c.done
}

func (c *Cache[K, V]) run(ctx context.Context) {
	defer close(c.done)

	err := c.Refresh(ctx)
	c.initialErr = err
	close(c.initialDone)
	if err != nil {
		c.reportBackgroundError(ctx, err)
	}
	if ctx.Err() != nil {
		return
	}

	var ticker *time.Ticker
	var ticks <-chan time.Time
	if c.refreshInterval > 0 {
		ticker = time.NewTicker(c.refreshInterval)
		ticks = ticker.C
		defer ticker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.notify:
			c.refreshInBackground(ctx)
		case <-ticks:
			c.refreshInBackground(ctx)
		}
	}
}

func (c *Cache[K, V]) refreshInBackground(ctx context.Context) {
	if err := c.Refresh(ctx); err != nil {
		c.reportBackgroundError(ctx, err)
	}
}

func (c *Cache[K, V]) reportBackgroundError(ctx context.Context, err error) {
	if c.errorHandler == nil || ctx.Err() != nil {
		return
	}
	c.errorHandler(ctx, err)
}

func (c *Cache[K, V]) load() map[K]V {
	pointer := c.data.Load()
	if pointer == nil {
		return nil
	}
	return *pointer
}

func cloneMap[K comparable, V any](source map[K]V) map[K]V {
	result := make(map[K]V, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
