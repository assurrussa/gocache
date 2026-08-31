package simple

import (
	"fmt"
	"time"
)

const (
	// DefaultTTL is the lifetime assigned to entries.
	DefaultTTL = 10 * time.Minute
	// DefaultCleanupInterval controls background removal of expired entries.
	DefaultCleanupInterval = time.Minute
)

type config struct {
	ttl             time.Duration
	cleanupInterval time.Duration
	metrics         Metrics
	now             func() time.Time
}

// Option configures a Cache. Options are created by this package's With
// functions.
type Option interface {
	apply(cfg *config) error
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
