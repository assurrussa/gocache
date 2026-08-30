package simple

import "errors"

var (
	// ErrNilCache reports a method call that requires a constructed Cache.
	ErrNilCache = errors.New("simple: cache is nil")
	// ErrNilContext reports a nil context passed to New or GetOrLoad.
	ErrNilContext = errors.New("simple: context must not be nil")
	// ErrEmptyName reports an empty cache name passed to New.
	ErrEmptyName = errors.New("simple: cache name must not be empty")
	// ErrNilLoader reports a nil Loader.
	ErrNilLoader = errors.New("simple: loader must not be nil")
	// ErrNilOption reports a nil functional option passed to New.
	ErrNilOption = errors.New("simple: option must not be nil")
	// ErrNilMetrics reports a nil metrics implementation.
	ErrNilMetrics = errors.New("simple: metrics must not be nil")
	// ErrInvalidTTL reports a non-positive TTL.
	ErrInvalidTTL = errors.New("simple: ttl must be positive")
	// ErrInvalidCleanupInterval reports a non-positive cleanup interval.
	ErrInvalidCleanupInterval = errors.New("simple: cleanup interval must be positive")
	// ErrLoaderPanic reports a panic recovered from a user loader.
	ErrLoaderPanic = errors.New("simple: loader panicked")
)
