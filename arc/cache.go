package arc

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	lruarc "github.com/hashicorp/golang-lru/arc/v2"

	"github.com/assurrussa/gocache/internal/flight"
)

type entry[V any] struct {
	value     V
	expiresAt time.Time
}

// Loader loads one value after a cache miss.
type Loader[V any] func(context.Context) (V, error)

// MultiLoader loads values for unique missing keys.
type MultiLoader[K comparable, V any] func(context.Context, []K) (map[K]V, error)

// Cache is a fixed-capacity Adaptive Replacement Cache with optional TTL.
// Construct a Cache with New; the zero value is not usable for mutations or
// loads.
type Cache[K comparable, V any] struct {
	name            string
	capacity        int
	ttl             time.Duration
	cleanupInterval time.Duration
	ttlJitter       time.Duration
	metrics         Metrics
	metricNames     metricNames
	cache           *lruarc.ARCCache[K, entry[V]]
	flights         flight.Group[K, V]
	done            chan struct{}

	mu     sync.RWMutex
	now    func() time.Time
	jitter func(time.Duration) time.Duration
}

// New constructs a context-bound ARC cache. Canceling ctx stops only the
// cleanup worker; reads, writes, and explicit loads remain available.
func New[K comparable, V any](ctx context.Context, name string, capacity int, options ...Option) (*Cache[K, V], error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	if strings.TrimSpace(name) == "" {
		return nil, ErrEmptyName
	}
	if capacity <= 0 {
		return nil, fmt.Errorf("%w: %d", ErrInvalidCapacity, capacity)
	}

	cfg := config{
		ttl:             DefaultTTL,
		cleanupInterval: DefaultCleanupInterval,
		ttlJitter:       DefaultTTLJitter,
		metrics:         noopMetrics{},
		now:             time.Now,
		jitter:          rand.N[time.Duration],
	}
	for _, option := range options {
		if option == nil {
			return nil, ErrNilOption
		}
		if err := option.apply(&cfg); err != nil {
			return nil, fmt.Errorf("arc: apply option: %w", err)
		}
	}

	backing, err := lruarc.NewARC[K, entry[V]](capacity)
	if err != nil {
		return nil, fmt.Errorf("arc: create backing cache: %w", err)
	}
	cache := &Cache[K, V]{
		name:            name,
		capacity:        capacity,
		ttl:             cfg.ttl,
		cleanupInterval: cfg.cleanupInterval,
		ttlJitter:       cfg.ttlJitter,
		metrics:         cfg.metrics,
		metricNames:     newMetricNames(name),
		cache:           backing,
		done:            make(chan struct{}),
		now:             cfg.now,
		jitter:          cfg.jitter,
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

// Capacity returns the maximum number of live ARC entries.
func (c *Cache[K, V]) Capacity() int {
	if c == nil {
		return 0
	}
	return c.capacity
}

// Len returns the number of physically stored entries. It may briefly include
// expired entries until a lookup or periodic cleanup removes them.
func (c *Cache[K, V]) Len() int {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cache.Len()
}

// Set inserts or replaces one value and resets its TTL.
func (c *Cache[K, V]) Set(key K, value V) {
	if c == nil {
		return
	}
	item := entry[V]{value: value, expiresAt: c.expiration(c.now())}
	c.mu.Lock()
	c.cache.Add(key, item)
	c.mu.Unlock()
}

func (c *Cache[K, V]) setLoaded(ctx context.Context, key K, value V) error {
	item := entry[V]{value: value, expiresAt: c.expiration(c.now())}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("arc: load canceled before publish: %w", err)
	}
	c.cache.Add(key, item)
	return nil
}

// Get returns a fresh value and updates ARC recency and frequency.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	return c.lookup(key, true, true)
}

// Peek returns a fresh value without updating ARC recency or frequency.
func (c *Cache[K, V]) Peek(key K) (V, bool) {
	return c.lookup(key, false, false)
}

// Contains reports whether a fresh value exists without updating ARC state.
func (c *Cache[K, V]) Contains(key K) bool {
	_, found := c.Peek(key)
	return found
}

// Keys returns physically stored keys in unspecified order. It may briefly
// include expired entries until cleanup.
func (c *Cache[K, V]) Keys() []K {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cache.Keys()
}

// Values returns physically stored values in unspecified order. It may
// briefly include expired entries until cleanup.
func (c *Cache[K, V]) Values() []V {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	items := c.cache.Values()
	c.mu.RUnlock()
	values := make([]V, len(items))
	for index, item := range items {
		values[index] = item.value
	}
	return values
}

// Delete removes a key if present.
func (c *Cache[K, V]) Delete(key K) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.cache.Remove(key)
	c.mu.Unlock()
	c.metrics.Increment(c.metricNames.deleted)
}

// Purge removes all entries.
func (c *Cache[K, V]) Purge() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.cache.Purge()
	c.mu.Unlock()
	c.metrics.Gauge(c.metricNames.length, 0)
}

// Done is closed after the constructor context stops the cleanup worker.
func (c *Cache[K, V]) Done() <-chan struct{} {
	if c == nil {
		return nil
	}
	return c.done
}

func (c *Cache[K, V]) lookup(key K, promote bool, recordAccess bool) (V, bool) {
	var zero V
	if c == nil {
		return zero, false
	}

	now := c.now()
	c.mu.Lock()
	var item entry[V]
	var found bool
	if promote {
		item, found = c.cache.Get(key)
	} else {
		item, found = c.cache.Peek(key)
	}
	if found && c.isExpired(item, now) {
		c.cache.Remove(key)
		c.mu.Unlock()
		if recordAccess {
			c.metrics.Increment(c.metricNames.expiredGet)
		}
		return zero, false
	}
	c.mu.Unlock()

	if !recordAccess {
		if !found {
			return zero, false
		}
		return item.value, true
	}
	if !found {
		c.metrics.Increment(c.metricNames.miss)
		return zero, false
	}
	c.metrics.Increment(c.metricNames.hit)
	return item.value, true
}

func (c *Cache[K, V]) expiration(now time.Time) time.Time {
	if c.ttl <= 0 {
		return time.Time{}
	}
	offset := time.Duration(0)
	if c.ttlJitter > 0 {
		offset = c.jitter(c.ttlJitter)
	}
	return now.Add(c.ttl).Add(offset)
}

func (c *Cache[K, V]) isExpired(item entry[V], now time.Time) bool {
	return !item.expiresAt.IsZero() && !now.Before(item.expiresAt)
}

func (c *Cache[K, V]) run(ctx context.Context) {
	defer close(c.done)
	if c.ttl <= 0 || c.cleanupInterval <= 0 {
		<-ctx.Done()
		return
	}

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
	keys := c.Keys()
	for _, key := range keys {
		select {
		case <-ctx.Done():
			return
		default:
		}

		removed := false
		c.mu.Lock()
		item, found := c.cache.Peek(key)
		if found && c.isExpired(item, now) {
			c.cache.Remove(key)
			removed = true
		}
		c.mu.Unlock()
		if removed {
			c.metrics.Increment(c.metricNames.expired)
		}
	}
	c.metrics.Gauge(c.metricNames.length, c.Len())
}
