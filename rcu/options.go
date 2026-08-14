package rcu

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	// DefaultRefreshInterval is the periodic refresh interval used by New.
	DefaultRefreshInterval = time.Hour
)

var (
	// ErrInvalidRefreshInterval reports a non-positive periodic interval.
	ErrInvalidRefreshInterval = errors.New("rcu: refresh interval must be positive")
	// ErrNilErrorHandler reports a nil handler passed to WithErrorHandler.
	ErrNilErrorHandler = errors.New("rcu: error handler must not be nil")
	// ErrNilOption reports a nil functional option passed to New.
	ErrNilOption = errors.New("rcu: option must not be nil")
)

// ErrorHandler receives failures from initial, periodic, and notified
// background refreshes. Synchronous Refresh errors are returned to its caller
// and are not sent to the handler.
type ErrorHandler func(context.Context, error)

type config struct {
	refreshInterval time.Duration
	errorHandler    ErrorHandler
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

// WithRefreshInterval configures periodic whole-snapshot refreshes.
func WithRefreshInterval(interval time.Duration) Option {
	return optionFunc(func(cfg *config) error {
		if interval <= 0 {
			return fmt.Errorf("%w: %s", ErrInvalidRefreshInterval, interval)
		}
		cfg.refreshInterval = interval
		return nil
	})
}

// WithoutPeriodicRefresh disables timer-driven refreshes. Explicit Refresh
// and event-driven Notify remain available.
func WithoutPeriodicRefresh() Option {
	return optionFunc(func(cfg *config) error {
		cfg.refreshInterval = 0
		return nil
	})
}

// WithErrorHandler configures the callback for background refresh failures.
func WithErrorHandler(handler ErrorHandler) Option {
	return optionFunc(func(cfg *config) error {
		if handler == nil {
			return ErrNilErrorHandler
		}
		cfg.errorHandler = handler
		return nil
	})
}
