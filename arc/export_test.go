package arc

import (
	"time"

	"github.com/assurrussa/gocache/internal/flight"
)

var (
	WithNow    = withNow
	WithJitter = withJitter
)

func (c *Cache[K, V]) PeekEntryExpiresAt(key K) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	item, found := c.cache.Peek(key)
	if !found {
		return time.Time{}, false
	}
	return item.expiresAt, true
}

func (c *Cache[K, V]) ClaimFlight(key K) flight.Claim[K, V] {
	return c.flights.Claim(key)
}
