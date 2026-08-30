package simple

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewValidatesInputsAndOptions(t *testing.T) {
	tests := []struct {
		name    string
		ctx     context.Context
		cache   string
		options []Option
		wantErr error
	}{
		{name: "nil context", cache: "test", wantErr: ErrNilContext},
		{name: "empty name", ctx: context.Background(), wantErr: ErrEmptyName},
		{name: "blank name", ctx: context.Background(), cache: "  ", wantErr: ErrEmptyName},
		{name: "nil option", ctx: context.Background(), cache: "test", options: []Option{nil}, wantErr: ErrNilOption},
		{name: "zero ttl", ctx: context.Background(), cache: "test", options: []Option{WithTTL(0)}, wantErr: ErrInvalidTTL},
		{name: "negative ttl", ctx: context.Background(), cache: "test", options: []Option{WithTTL(-time.Second)}, wantErr: ErrInvalidTTL},
		{name: "zero cleanup", ctx: context.Background(), cache: "test", options: []Option{WithCleanupInterval(0)}, wantErr: ErrInvalidCleanupInterval},
		{name: "nil metrics", ctx: context.Background(), cache: "test", options: []Option{WithMetrics(nil)}, wantErr: ErrNilMetrics},
		{name: "typed nil metrics", ctx: context.Background(), cache: "test", options: []Option{WithMetrics((*metricRecorder)(nil))}, wantErr: ErrNilMetrics},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache, err := New[string, int](test.ctx, test.cache, test.options...)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("New() error = %v, want %v", err, test.wantErr)
			}
			if cache != nil {
				t.Fatal("New() returned cache for invalid input")
			}
		})
	}
}

func TestCacheBasicOperationsAndNilReceiver(t *testing.T) {
	var nilCache *Cache[int, int]
	if nilCache.Name() != "" || nilCache.Len() != 0 {
		t.Fatal("nil cache metadata must use zero values")
	}
	if _, found := nilCache.Get(1); found {
		t.Fatal("nil cache Get found a value")
	}
	if nilCache.Keys() != nil || nilCache.Values() != nil || nilCache.Done() != nil {
		t.Fatal("nil cache collections or Done must be nil")
	}
	nilCache.Set(1, 1)
	if _, err := nilCache.GetOrLoad(context.Background(), 1, func(context.Context) (int, error) { return 1, nil }); !errors.Is(err, ErrNilCache) {
		t.Fatalf("nil GetOrLoad() error = %v", err)
	}

	cache := newTestCache[int, int](t, "basic", WithCleanupInterval(time.Hour))
	if cache.Name() != "basic" {
		t.Fatalf("Name() = %q", cache.Name())
	}
	cache.Set(1, 10)
	cache.Set(2, 20)
	cache.Set(1, 11)
	if cache.Len() != 2 {
		t.Fatalf("Len() = %d", cache.Len())
	}
	if value, found := cache.Get(1); !found || value != 11 {
		t.Fatalf("Get(1) = %d, %t", value, found)
	}
	keys := cache.Keys()
	slices.Sort(keys)
	if !slices.Equal(keys, []int{1, 2}) {
		t.Fatalf("Keys() = %v", keys)
	}
	values := cache.Values()
	slices.Sort(values)
	if !slices.Equal(values, []int{11, 20}) {
		t.Fatalf("Values() = %v", values)
	}
}

func TestTTLIsStrictOnGetAndEventualOnViews(t *testing.T) {
	base := time.Unix(100, 0)
	clock := newTestClock(base)
	cache := newTestCache[int, int](t, "ttl", WithTTL(10*time.Second), WithCleanupInterval(time.Hour), withNow(clock.Now))
	cache.Set(1, 10)
	clock.Set(base.Add(10 * time.Second))
	if cache.Len() != 1 || len(cache.Keys()) != 1 || len(cache.Values()) != 1 {
		t.Fatal("collection views did not preserve eventual-expiration semantics")
	}
	if _, found := cache.Get(1); found {
		t.Fatal("Get returned expired value")
	}
	if cache.Len() != 0 {
		t.Fatalf("Len() after expired Get = %d", cache.Len())
	}
}

func TestCleanupLifecycleAndMetrics(t *testing.T) {
	metrics := newMetricRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	clock := newTestClock(time.Unix(200, 0))
	cache, err := New[int, int](ctx, "cleanup",
		WithTTL(time.Second),
		WithCleanupInterval(time.Millisecond),
		WithMetrics(metrics),
		withNow(clock.Now),
	)
	if err != nil {
		t.Fatal(err)
	}
	cache.Set(1, 1)
	cache.Set(2, 2)
	clock.Set(time.Unix(202, 0))
	eventually(t, time.Second, func() bool {
		return cache.Len() == 0 && metrics.count("cache.cleanup.expired") == 2
	})
	if metrics.count("cache.cleanup.expired") != 2 {
		t.Fatalf("expired metric = %d", metrics.count("cache.cleanup.expired"))
	}
	if metrics.gauge("cache.cleanup.len") != 0 {
		t.Fatalf("len gauge = %v", metrics.gauge("cache.cleanup.len"))
	}

	cancel()
	select {
	case <-cache.Done():
	case <-time.After(time.Second):
		t.Fatal("cleanup worker did not stop")
	}
	clock.Set(time.Now())
	cache.Set(3, 3)
	if value, found := cache.Get(3); !found || value != 3 {
		t.Fatalf("cache unusable after Done: %d, %t", value, found)
	}
}

func TestAccessMetrics(t *testing.T) {
	metrics := newMetricRecorder()
	base := time.Unix(300, 0)
	clock := newTestClock(base)
	cache := newTestCache[int, int](t, "metrics",
		WithTTL(time.Second),
		WithCleanupInterval(time.Hour),
		WithMetrics(metrics),
		withNow(clock.Now),
	)
	cache.Set(1, 1)
	cache.Get(1)
	cache.Get(2)
	clock.Set(base.Add(2 * time.Second))
	cache.Get(1)
	wants := map[string]int{
		"cache.metrics.hit":         1,
		"cache.metrics.miss":        1,
		"cache.metrics.expired_get": 1,
	}
	for name, want := range wants {
		if got := metrics.count(name); got != want {
			t.Errorf("metric %s = %d, want %d", name, got, want)
		}
	}
}

func TestGetOrLoadValuesErrorsPanicsAndCancellation(t *testing.T) {
	cache := newTestCache[int, int](t, "load", WithCleanupInterval(time.Hour))
	if value, err := cache.GetOrLoad(context.Background(), 1, func(context.Context) (int, error) { return 0, nil }); err != nil || value != 0 {
		t.Fatalf("zero value load = %d, %v", value, err)
	}
	if value, found := cache.Get(1); !found || value != 0 {
		t.Fatalf("cached zero value = %d, %t", value, found)
	}
	wantErr := errors.New("source failed")
	if _, err := cache.GetOrLoad(context.Background(), 2, func(context.Context) (int, error) { return 0, wantErr }); !errors.Is(err, wantErr) {
		t.Fatalf("loader error = %v", err)
	}
	if _, err := cache.GetOrLoad(context.Background(), 3, func(context.Context) (int, error) { panic("boom") }); !errors.Is(err, ErrLoaderPanic) {
		t.Fatalf("panic error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := cache.GetOrLoad(ctx, 4, func(context.Context) (int, error) {
		cancel()
		return 4, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled load error = %v", err)
	}
	if _, found := cache.Get(4); found {
		t.Fatal("canceled value was cached")
	}
	if _, err := cache.GetOrLoad(nil, 5, func(context.Context) (int, error) { //nolint:staticcheck // Verifies the explicit nil-context contract.
		return 5, nil
	}); !errors.Is(err, ErrNilContext) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := cache.GetOrLoad(context.Background(), 5, nil); !errors.Is(err, ErrNilLoader) {
		t.Fatalf("nil loader error = %v", err)
	}

	pointers := newTestCache[int, *int](t, "nil-value", WithCleanupInterval(time.Hour))
	value, err := pointers.GetOrLoad(context.Background(), 1, func(context.Context) (*int, error) { return nil, nil })
	if err != nil || value != nil {
		t.Fatalf("nil pointer load = %v, %v", value, err)
	}
	if cached, found := pointers.Get(1); !found || cached != nil {
		t.Fatalf("cached nil pointer = %v, %t", cached, found)
	}
}

func TestGetOrLoadCoalescingParallelismAndWaiterCancellation(t *testing.T) {
	cache := newTestCache[int, int](t, "coalesce", WithCleanupInterval(time.Hour))
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	loader := func(context.Context) (int, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return 42, nil
	}
	const callers = 24
	results := make(chan error, callers)
	for range callers {
		go func() {
			value, err := cache.GetOrLoad(context.Background(), 1, loader)
			if err == nil && value != 42 {
				err = errors.New("unexpected value")
			}
			results <- err
		}()
	}
	<-started
	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := cache.GetOrLoad(waitCtx, 1, func(context.Context) (int, error) { return 2, nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter error = %v", err)
	}
	close(release)
	for range callers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("same-key loader calls = %d, want 1", calls.Load())
	}

	parallel := newTestCache[int, int](t, "parallel", WithCleanupInterval(time.Hour))
	startedKeys := make(chan int, 2)
	releaseBoth := make(chan struct{})
	result := make(chan error, 2)
	for _, key := range []int{1, 2} {
		go func(key int) {
			_, err := parallel.GetOrLoad(context.Background(), key, func(context.Context) (int, error) {
				startedKeys <- key
				<-releaseBoth
				return key, nil
			})
			result <- err
		}(key)
	}
	seen := map[int]bool{}
	for range 2 {
		select {
		case key := <-startedKeys:
			seen[key] = true
		case <-time.After(time.Second):
			t.Fatal("different-key loaders did not run in parallel")
		}
	}
	close(releaseBoth)
	for range 2 {
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	}
	if !seen[1] || !seen[2] {
		t.Fatalf("started keys = %v", seen)
	}
}

func TestConcurrentOperations(t *testing.T) {
	cache := newTestCache[int, int](t, "race", WithCleanupInterval(time.Hour))
	var wait sync.WaitGroup
	for worker := range 12 {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for index := range 2_000 {
				key := (worker + index) % 128
				cache.Set(key, index)
				cache.Get(key)
				cache.Len()
				if index%31 == 0 {
					cache.Keys()
					cache.Values()
				}
			}
		}(worker)
	}
	wait.Wait()
	if cache.Len() == 0 {
		t.Fatal("concurrent operations left cache unexpectedly empty")
	}
}

func newTestCache[K comparable, V any](t *testing.T, name string, options ...Option) *Cache[K, V] {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cache, err := New[K, V](ctx, name, options...)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-cache.Done():
		case <-time.After(time.Second):
			t.Error("cache worker did not stop")
		}
	})
	return cache
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !condition() {
		t.Fatal("condition was not met before timeout")
	}
}

type metricRecorder struct {
	mu         sync.Mutex
	increments map[string]int
	gauges     map[string]any
}

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock(now time.Time) *testClock {
	return &testClock{now: now}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Set(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

func newMetricRecorder() *metricRecorder {
	return &metricRecorder{increments: make(map[string]int), gauges: make(map[string]any)}
}

func (m *metricRecorder) Increment(key string) {
	m.mu.Lock()
	m.increments[key]++
	m.mu.Unlock()
}

func (m *metricRecorder) Gauge(key string, value any) {
	m.mu.Lock()
	m.gauges[key] = value
	m.mu.Unlock()
}

func (m *metricRecorder) count(key string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.increments[key]
}

func (m *metricRecorder) gauge(key string) any {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gauges[key]
}
