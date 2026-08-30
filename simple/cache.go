package simple

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/assurrussa/gocache/internal/flight"
)

type entry[V any] struct {
	value     V
	expiresAt time.Time
}

// Loader loads one value after a cache miss.
type Loader[V any] func(context.Context) (V, error)

// Cache is an unbounded concurrent map with fixed per-entry TTL and
// cache-aside duplicate-load suppression.
type Cache[K comparable, V any] struct {
	name            string
	ttl             time.Duration
	cleanupInterval time.Duration
	metrics         Metrics
	metricNames     metricNames
	data            map[K]entry[V]
	flights         flight.Group[K, V]
	done            chan struct{}

	mu  sync.RWMutex
	now func() time.Time
}

// New constructs a context-bound TTL cache. Canceling ctx stops only the
// cleanup worker; reads, writes, and explicit loads remain available.
func New[K comparable, V any](ctx context.Context, name string, options ...Option) (*Cache[K, V], error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if strings.TrimSpace(name) == "" {
		return nil, ErrEmptyName
	}
	cfg := config{
		ttl:             DefaultTTL,
		cleanupInterval: DefaultCleanupInterval,
		metrics:         noopMetrics{},
		now:             time.Now,
	}
	for _, option := range options {
		if option == nil {
			return nil, ErrNilOption
		}
		if err := option.apply(&cfg); err != nil {
			return nil, fmt.Errorf("simple: apply option: %w", err)
		}
	}
	cache := &Cache[K, V]{
		name:            name,
		ttl:             cfg.ttl,
		cleanupInterval: cfg.cleanupInterval,
		metrics:         cfg.metrics,
		metricNames:     newMetricNames(name),
		data:            make(map[K]entry[V]),
		done:            make(chan struct{}),
		now:             cfg.now,
	}
	go cache.run(ctx)
	return cache, nil
}

// Name returns the configured cache name.
func (c *Cache[K, V]) Name() string {
	if c == nil {
		return ""
	}
	return c.name
}

// Set inserts or replaces one value and resets its TTL.
func (c *Cache[K, V]) Set(key K, value V) {
	if c == nil {
		return
	}
	item := entry[V]{value: value, expiresAt: c.now().Add(c.ttl)}
	c.mu.Lock()
	c.data[key] = item
	c.mu.Unlock()
}

func (c *Cache[K, V]) setLoaded(ctx context.Context, key K, value V) error {
	item := entry[V]{value: value, expiresAt: c.now().Add(c.ttl)}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("simple: load canceled before publish: %w", err)
	}
	c.data[key] = item
	return nil
}

// Get returns a fresh value.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	return c.lookup(key, true)
}

// Len returns the number of physically stored entries. It may briefly include
// expired entries until a lookup or periodic cleanup removes them.
func (c *Cache[K, V]) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.data)
}

// Keys returns physically stored keys in unspecified order. It may briefly
// include expired entries until cleanup.
func (c *Cache[K, V]) Keys() []K {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	keys := make([]K, 0, len(c.data))
	for key := range c.data {
		keys = append(keys, key)
	}
	return keys
}

// Values returns physically stored values in unspecified order. It may
// briefly include expired entries until cleanup.
func (c *Cache[K, V]) Values() []V {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	values := make([]V, 0, len(c.data))
	for _, item := range c.data {
		values = append(values, item.value)
	}
	return values
}

// GetOrLoad returns a cached value or coalesces one loader call for key.
func (c *Cache[K, V]) GetOrLoad(ctx context.Context, key K, loader Loader[V]) (V, error) {
	var zero V
	if c == nil {
		return zero, ErrNilCache
	}
	if ctx == nil {
		return zero, ErrNilContext
	}
	if loader == nil {
		return zero, ErrNilLoader
	}
	if err := ctx.Err(); err != nil {
		return zero, fmt.Errorf("simple: get or load canceled: %w", err)
	}
	if value, found := c.Get(key); found {
		return value, nil
	}

	claim := c.flights.Claim(key)
	if claim.Owned() {
		if value, found := c.lookup(key, false); found {
			claim.Complete(flight.Result[V]{Value: value, Found: true})
		} else {
			value, err := invokeLoader(ctx, loader)
			if err == nil {
				err = c.setLoaded(ctx, key, value)
			}
			claim.Complete(flight.Result[V]{Value: value, Found: err == nil, Err: err})
		}
	}
	result, err := claim.Wait(ctx)
	if err != nil {
		return zero, fmt.Errorf("simple: wait for key load: %w", err)
	}
	if result.Err != nil {
		return zero, fmt.Errorf("simple: get or load: %w", result.Err)
	}
	return result.Value, nil
}

// Done is closed after the constructor context stops the cleanup worker.
func (c *Cache[K, V]) Done() <-chan struct{} {
	if c == nil {
		return nil
	}
	return c.done
}

func (c *Cache[K, V]) lookup(key K, recordAccess bool) (V, bool) {
	var zero V
	if c == nil {
		return zero, false
	}

	now := c.now()
	c.mu.RLock()
	item, found := c.data[key]
	if found && now.Before(item.expiresAt) {
		c.mu.RUnlock()
		if recordAccess {
			c.metrics.Increment(c.metricNames.hit)
		}
		return item.value, true
	}
	c.mu.RUnlock()
	if !found {
		if recordAccess {
			c.metrics.Increment(c.metricNames.miss)
		}
		return zero, false
	}

	// Recheck under the write lock so a concurrent Set cannot be deleted by
	// an expiration decision made from an older entry.
	c.mu.Lock()
	item, found = c.data[key]
	if found && !now.Before(item.expiresAt) {
		delete(c.data, key)
		found = false
	}
	c.mu.Unlock()
	if found {
		if recordAccess {
			c.metrics.Increment(c.metricNames.hit)
		}
		return item.value, true
	}
	if recordAccess {
		c.metrics.Increment(c.metricNames.expiredGet)
	}
	return zero, false
}

func (c *Cache[K, V]) run(ctx context.Context) {
	defer close(c.done)
	ticker := time.NewTicker(c.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.removeExpired(ctx)
		}
	}
}

func (c *Cache[K, V]) removeExpired(ctx context.Context) {
	now := c.now()
	removed := 0
	c.mu.Lock()
	for key, item := range c.data {
		select {
		case <-ctx.Done():
			c.mu.Unlock()
			return
		default:
		}
		if !now.Before(item.expiresAt) {
			delete(c.data, key)
			removed++
		}
	}
	length := len(c.data)
	c.mu.Unlock()
	for range removed {
		c.metrics.Increment(c.metricNames.expired)
	}
	c.metrics.Gauge(c.metricNames.length, length)
}

func invokeLoader[V any](ctx context.Context, loader Loader[V]) (value V, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			var zero V
			value = zero
			err = fmt.Errorf("%w: %v", ErrLoaderPanic, recovered)
		}
	}()
	value, err = loader(ctx)
	if err != nil {
		var zero V
		return zero, fmt.Errorf("simple: loader: %w", err)
	}
	if err := ctx.Err(); err != nil {
		var zero V
		return zero, fmt.Errorf("simple: loader canceled: %w", err)
	}
	return value, nil
}
