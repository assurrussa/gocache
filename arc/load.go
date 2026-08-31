package arc

import (
	"context"
	"fmt"
	"slices"

	"github.com/assurrussa/gocache/internal/flight"
)

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
		return zero, fmt.Errorf("arc: get or load canceled: %w", err)
	}
	if value, found := c.Get(key); found {
		return value, nil
	}

	for {
		claim := c.flights.Claim(key)
		if claim.Owned() {
			if value, found := c.lookup(key, true, false); found {
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
			return zero, fmt.Errorf("arc: wait for key load: %w", err)
		}
		if result.Err != nil {
			return zero, fmt.Errorf("arc: get or load: %w", result.Err)
		}
		if result.Found {
			return result.Value, nil
		}
		if err := ctx.Err(); err != nil {
			return zero, fmt.Errorf("arc: get or load canceled: %w", err)
		}
		// A joined batch may legitimately omit this key. Retry so this
		// single-value loader still has a chance to supply it.
	}
}

// GetOrLoadMany returns cached values and batch-loads unique missing keys.
// Overlapping single and batch calls coalesce by key.
// When a joined load omits a key, the loader may be called again for every
// still-missing key because the prior round's staged values are discarded.
func (c *Cache[K, V]) GetOrLoadMany(ctx context.Context, keys []K, loader MultiLoader[K, V]) (map[K]V, error) {
	if c == nil {
		return nil, ErrNilCache
	}
	if ctx == nil {
		return nil, ErrNilContext
	}
	if loader == nil {
		return nil, ErrNilLoader
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("arc: get or load many canceled: %w", err)
	}

	unique := uniqueKeys(keys)
	values := make(map[K]V, len(unique))
	if len(unique) == 0 {
		return values, nil
	}
	missing := make([]K, 0, len(unique))
	for _, key := range unique {
		if value, found := c.Get(key); found {
			values[key] = value
			continue
		}
		missing = append(missing, key)
	}
	if len(missing) == 0 {
		return values, nil
	}

	pending := missing
	for len(pending) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("arc: get or load many canceled: %w", err)
		}

		claims, owned, ownedKeys, loaded, err := c.claimAndLoadOwned(ctx, pending, values, loader)
		if err != nil {
			return nil, err
		}

		retry, err := c.waitJoinedClaims(ctx, claims, pending, values, owned)
		if err != nil {
			return nil, err
		}
		if retry {
			// Release every claim from this attempt before retrying. Retaining
			// any owned claim while acquiring omitted joined keys could create
			// a cycle with another overlapping batch. Staged values are not
			// published, so all still-missing keys are safely retried together.
			completeWithoutValue(owned)
			pending = c.remainingMissing(pending, values)
			continue
		}

		if err := c.publishOwnedBatch(ctx, owned, ownedKeys, loaded, values); err != nil {
			return nil, err
		}
		return values, nil
	}
	return values, nil
}

func (c *Cache[K, V]) claimAndLoadOwned(
	ctx context.Context,
	pending []K,
	values map[K]V,
	loader MultiLoader[K, V],
) (
	claims []flight.Claim[K, V],
	owned []flight.Claim[K, V],
	ownedKeys []K,
	loaded map[K]V,
	err error,
) {
	claims = c.flights.ClaimMany(pending)
	loadKeys := make([]K, 0, len(pending))
	ownedKeys = make([]K, 0, len(pending))
	owned = make([]flight.Claim[K, V], 0, len(pending))
	for index, claim := range claims {
		if !claim.Owned() {
			continue
		}
		key := pending[index]
		if value, found := c.lookup(key, true, false); found {
			values[key] = value
			claim.Complete(flight.Result[V]{Value: value, Found: true})
			continue
		}
		loadKeys = append(loadKeys, key)
		ownedKeys = append(ownedKeys, key)
		owned = append(owned, claim)
	}

	loaded = make(map[K]V, len(loadKeys))
	if len(loadKeys) > 0 {
		round, loadErr := invokeMultiLoader(ctx, loader, loadKeys)
		if loadErr != nil {
			completeWithError(owned, loadErr)
			return nil, nil, nil, nil, fmt.Errorf("arc: get or load many: %w", loadErr)
		}
		for _, key := range loadKeys {
			if value, found := round[key]; found {
				loaded[key] = value
			}
		}
	}
	return claims, owned, ownedKeys, loaded, nil
}

func (c *Cache[K, V]) waitJoinedClaims(
	ctx context.Context,
	claims []flight.Claim[K, V],
	pending []K,
	values map[K]V,
	owned []flight.Claim[K, V],
) (bool, error) {
	retry := false
	for index, claim := range claims {
		if claim.Owned() {
			continue
		}
		result, err := claim.Wait(ctx)
		if err != nil {
			wrapped := fmt.Errorf("arc: wait for batch load: %w", err)
			completeWithError(owned, wrapped)
			return false, wrapped
		}
		if result.Err != nil {
			completeWithError(owned, result.Err)
			return false, fmt.Errorf("arc: get or load many: %w", result.Err)
		}
		key := pending[index]
		if result.Found {
			values[key] = result.Value
			continue
		}
		retry = true
	}
	return retry, nil
}

func (c *Cache[K, V]) publishOwnedBatch(
	ctx context.Context,
	owned []flight.Claim[K, V],
	ownedKeys []K,
	loaded map[K]V,
	values map[K]V,
) error {
	if err := ctx.Err(); err != nil {
		wrapped := fmt.Errorf("arc: get or load many canceled: %w", err)
		completeWithError(owned, wrapped)
		return wrapped
	}
	results := make([]flight.Result[V], len(ownedKeys))
	items := make([]entry[V], len(ownedKeys))
	now := c.now()
	for index, key := range ownedKeys {
		value, found := loaded[key]
		if found {
			items[index] = entry[V]{value: value, expiresAt: c.expiration(now)}
		}
		results[index] = flight.Result[V]{Value: value, Found: found}
	}

	c.mu.Lock()
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		wrapped := fmt.Errorf("arc: get or load many canceled before publish: %w", err)
		completeWithError(owned, wrapped)
		return wrapped
	}
	for index, key := range ownedKeys {
		if results[index].Found {
			c.cache.Add(key, items[index])
		}
	}
	c.mu.Unlock()

	for index, key := range ownedKeys {
		if results[index].Found {
			values[key] = results[index].Value
		}
		owned[index].Complete(results[index])
	}
	return nil
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
		return zero, fmt.Errorf("arc: loader: %w", err)
	}
	if err := ctx.Err(); err != nil {
		var zero V
		return zero, fmt.Errorf("arc: loader canceled: %w", err)
	}
	return value, nil
}

func invokeMultiLoader[K comparable, V any](ctx context.Context, loader MultiLoader[K, V], keys []K) (values map[K]V, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			values = nil
			err = fmt.Errorf("%w: %v", ErrLoaderPanic, recovered)
		}
	}()
	values, err = loader(ctx, slices.Clone(keys))
	if err != nil {
		return nil, fmt.Errorf("arc: multi loader: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("arc: multi loader canceled: %w", err)
	}
	return values, nil
}

func completeWithError[K comparable, V any](claims []flight.Claim[K, V], err error) {
	for _, claim := range claims {
		claim.Complete(flight.Result[V]{Err: err})
	}
}

func completeWithoutValue[K comparable, V any](claims []flight.Claim[K, V]) {
	for _, claim := range claims {
		claim.Complete(flight.Result[V]{})
	}
}

func (c *Cache[K, V]) remainingMissing(keys []K, values map[K]V) []K {
	missing := make([]K, 0, len(keys))
	for _, key := range keys {
		if _, found := values[key]; found {
			continue
		}
		if value, found := c.lookup(key, true, false); found {
			values[key] = value
			continue
		}
		missing = append(missing, key)
	}
	return missing
}

func uniqueKeys[K comparable](keys []K) []K {
	seen := make(map[K]struct{}, len(keys))
	unique := make([]K, 0, len(keys))
	for _, key := range keys {
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, key)
	}
	return unique
}
