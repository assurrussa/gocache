// Package flight provides generic duplicate-load suppression for cache keys.
package flight

import (
	"context"
	"sync"
)

// Result is the outcome shared by callers waiting for the same key.
type Result[V any] struct {
	Value V
	Found bool
	Err   error
}

type call[V any] struct {
	done   chan struct{}
	result Result[V]
}

// Claim represents one key in an in-flight load. Exactly one claim for a key
// is owned; all other claims wait for the owner's result.
type Claim[K comparable, V any] struct {
	group *Group[K, V]
	key   K
	call  *call[V]
	owned bool
}

// Owned reports whether this claim is responsible for completing the load.
func (c Claim[K, V]) Owned() bool {
	return c.owned
}

// Complete publishes the result to all waiters. It is safe to call more than
// once; only the first completion for the active claim takes effect.
func (c Claim[K, V]) Complete(result Result[V]) {
	if !c.owned || c.group == nil || c.call == nil {
		return
	}

	c.group.mu.Lock()
	defer c.group.mu.Unlock()
	if c.group.calls[c.key] != c.call {
		return
	}
	c.call.result = result
	delete(c.group.calls, c.key)
	close(c.call.done)
}

// Wait waits for the owner or for the caller's context to be canceled.
func (c Claim[K, V]) Wait(ctx context.Context) (Result[V], error) {
	select {
	case <-c.call.done:
		return c.call.result, nil
	default:
	}

	select {
	case <-c.call.done:
		return c.call.result, nil
	case <-ctx.Done():
		return Result[V]{}, ctx.Err()
	}
}

// Group tracks loads currently running for comparable keys. Its zero value is
// ready for use.
type Group[K comparable, V any] struct {
	mu    sync.Mutex
	calls map[K]*call[V]
}

// Claim returns an owner or waiter claim for key.
func (g *Group[K, V]) Claim(key K) Claim[K, V] {
	claims := g.ClaimMany([]K{key})
	return claims[0]
}

// ClaimMany claims every key while holding one lock. Callers should pass
// unique keys. Atomic claiming prevents overlapping batches from owning keys
// in a cycle.
func (g *Group[K, V]) ClaimMany(keys []K) []Claim[K, V] {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.calls == nil {
		g.calls = make(map[K]*call[V])
	}
	claims := make([]Claim[K, V], len(keys))
	for index, key := range keys {
		if existing, ok := g.calls[key]; ok {
			claims[index] = Claim[K, V]{group: g, key: key, call: existing}
			continue
		}
		created := &call[V]{done: make(chan struct{})}
		g.calls[key] = created
		claims[index] = Claim[K, V]{group: g, key: key, call: created, owned: true}
	}
	return claims
}
