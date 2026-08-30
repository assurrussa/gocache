package arc

import (
	"fmt"
	"time"
)

const (
	// DefaultTTL is the lifetime assigned to ARC entries.
	DefaultTTL = 5 * time.Minute
	// DefaultCleanupInterval controls background removal of expired entries.
	DefaultCleanupInterval = time.Minute
	// DefaultTTLJitter spreads entry expiration over this maximum offset.
	DefaultTTLJitter = time.Minute
)

type config struct {
	ttl             time.Duration
	cleanupInterval time.Duration
	ttlJitter       time.Duration
	metrics         Metrics
	now             func() time.Time
	jitter          func(time.Duration) time.Duration
}

// Option configures a Cache. Options are created by this package's With and
// Without functions.
type Option interface {
	apply(*config) error
}

type optionFunc func(*config) error

func (option optionFunc) apply(cfg *config) error {
	return option(cfg)
}

// WithTTL configures entry lifetime.
func WithTTL(ttl time.Duration) Option {
	return optionFunc(func(cfg *config) error {
		if ttl <= 0 {
			return fmt.Errorf("%w: %s", ErrInvalidTTL, ttl)
		}
		cfg.ttl = ttl
		return nil
	})
}

// WithoutExpiration disables TTL checks and background expiration.
func WithoutExpiration() Option {
	return optionFunc(func(cfg *config) error {
		cfg.ttl = 0
		return nil
	})
}

// WithCleanupInterval configures periodic expired-entry cleanup.
func WithCleanupInterval(interval time.Duration) Option {
	return optionFunc(func(cfg *config) error {
		if interval <= 0 {
			return fmt.Errorf("%w: %s", ErrInvalidCleanupInterval, interval)
		}
		cfg.cleanupInterval = interval
		return nil
	})
}

// WithoutPeriodicCleanup disables timer-driven cleanup. Key lookups still
// reject expired entries.
func WithoutPeriodicCleanup() Option {
	return optionFunc(func(cfg *config) error {
		cfg.cleanupInterval = 0
		return nil
	})
}

// WithTTLJitter configures the maximum random offset added to each TTL.
func WithTTLJitter(maximum time.Duration) Option {
	return optionFunc(func(cfg *config) error {
		if maximum <= 0 {
			return fmt.Errorf("%w: %s", ErrInvalidTTLJitter, maximum)
		}
		cfg.ttlJitter = maximum
		return nil
	})
}

// WithoutTTLJitter disables expiration spreading.
func WithoutTTLJitter() Option {
	return optionFunc(func(cfg *config) error {
		cfg.ttlJitter = 0
		return nil
	})
}

// WithMetrics configures optional cache metrics.
func WithMetrics(metrics Metrics) Option {
	return optionFunc(func(cfg *config) error {
		if isNilMetrics(metrics) {
			return ErrNilMetrics
		}
		cfg.metrics = metrics
		return nil
	})
}

func withNow(now func() time.Time) Option {
	return optionFunc(func(cfg *config) error {
		cfg.now = now
		return nil
	})
}

func withJitter(jitter func(time.Duration) time.Duration) Option {
	return optionFunc(func(cfg *config) error {
		cfg.jitter = jitter
		return nil
	})
}
