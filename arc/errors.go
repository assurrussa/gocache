package arc

import "errors"

var (
	// ErrNilCache reports a method call that requires a constructed Cache.
	ErrNilCache = errors.New("arc: cache is nil")
	// ErrNilContext reports a nil context passed to New or a load operation.
	ErrNilContext = errors.New("arc: context must not be nil")
	// ErrEmptyName reports an empty cache name passed to New.
	ErrEmptyName = errors.New("arc: cache name must not be empty")
	// ErrInvalidCapacity reports a non-positive ARC capacity.
	ErrInvalidCapacity = errors.New("arc: capacity must be positive")
	// ErrNilLoader reports a nil Loader or MultiLoader.
	ErrNilLoader = errors.New("arc: loader must not be nil")
	// ErrNilOption reports a nil functional option passed to New.
	ErrNilOption = errors.New("arc: option must not be nil")
	// ErrNilMetrics reports a nil metrics implementation.
	ErrNilMetrics = errors.New("arc: metrics must not be nil")
	// ErrInvalidTTL reports a non-positive TTL passed to WithTTL.
	ErrInvalidTTL = errors.New("arc: ttl must be positive")
	// ErrInvalidCleanupInterval reports a non-positive cleanup interval.
	ErrInvalidCleanupInterval = errors.New("arc: cleanup interval must be positive")
	// ErrInvalidTTLJitter reports a non-positive maximum TTL jitter.
	ErrInvalidTTLJitter = errors.New("arc: ttl jitter must be positive")
	// ErrLoaderPanic reports a panic recovered from a user loader.
	ErrLoaderPanic = errors.New("arc: loader panicked")
)
