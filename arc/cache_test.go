package arc_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/assurrussa/gocache/arc"
)

const testCacheName = "test"

func TestNewValidatesInputsAndOptions(t *testing.T) {
	tests := []struct {
		name     string
		nilCtx   bool
		cache    string
		capacity int
		options  []arc.Option
		wantErr  error
	}{
		{name: "nil context", nilCtx: true, cache: testCacheName, capacity: 1, wantErr: arc.ErrNilContext},
		{name: "empty name", capacity: 1, wantErr: arc.ErrEmptyName},
		{name: "blank name", cache: "  ", capacity: 1, wantErr: arc.ErrEmptyName},
		{name: "zero capacity", cache: testCacheName, wantErr: arc.ErrInvalidCapacity},
		{name: "nil option", cache: testCacheName, capacity: 1, options: []arc.Option{nil}, wantErr: arc.ErrNilOption},
		{
			name:     "zero ttl",
			cache:    testCacheName,
			capacity: 1,
			options:  []arc.Option{arc.WithTTL(0)},
			wantErr:  arc.ErrInvalidTTL,
		},
		{
			name:     "zero cleanup",
			cache:    testCacheName,
			capacity: 1,
			options:  []arc.Option{arc.WithCleanupInterval(0)},
			wantErr:  arc.ErrInvalidCleanupInterval,
		},
		{
			name:     "zero jitter",
			cache:    testCacheName,
			capacity: 1,
			options:  []arc.Option{arc.WithTTLJitter(0)},
			wantErr:  arc.ErrInvalidTTLJitter,
		},
		{
			name:     "nil metrics",
			cache:    testCacheName,
			capacity: 1,
			options:  []arc.Option{arc.WithMetrics(nil)},
			wantErr:  arc.ErrNilMetrics,
		},
		{
			name:     "typed nil metrics",
			cache:    testCacheName,
			capacity: 1,
			options:  []arc.Option{arc.WithMetrics((*metricRecorder)(nil))},
			wantErr:  arc.ErrNilMetrics,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			if test.nilCtx {
				ctx = nil
			}
			cache, err := arc.New[string, int](ctx, test.cache, test.capacity, test.options...)
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
	var nilCache *arc.Cache[int, int]
	if nilCache.Name() != "" || nilCache.Capacity() != 0 || nilCache.Len() != 0 {
		t.Fatal("nil cache metadata must use zero values")
	}
	if _, found := nilCache.Get(1); found {
		t.Fatal("nil cache Get found a value")
	}
	if _, found := nilCache.Peek(1); found || nilCache.Contains(1) {
		t.Fatal("nil cache lookup found a value")
	}
	if nilCache.Keys() != nil || nilCache.Values() != nil || nilCache.Done() != nil {
		t.Fatal("nil cache collections or Done must be nil")
	}
	nilCache.Set(1, 1)
	nilCache.Delete(1)
	nilCache.Purge()
	if _, err := nilCache.GetOrLoad(
		context.Background(),
		1,
		func(context.Context) (int, error) { return 1, nil },
	); !errors.Is(err, arc.ErrNilCache) {
		t.Fatalf("nil GetOrLoad() error = %v", err)
	}
	if _, err := nilCache.GetOrLoadMany(
		context.Background(),
		[]int{1},
		func(context.Context, []int) (map[int]int, error) { return map[int]int{}, nil },
	); !errors.Is(err, arc.ErrNilCache) {
		t.Fatalf("nil GetOrLoadMany() error = %v", err)
	}

	cache := newTestCache[int, int](t, "basic", 2, arc.WithoutExpiration(), arc.WithoutPeriodicCleanup())
	if cache.Name() != "basic" || cache.Capacity() != 2 {
		t.Fatalf("metadata = %q/%d", cache.Name(), cache.Capacity())
	}
	cache.Set(1, 10)
	cache.Set(2, 20)
	if value, found := cache.Get(1); !found || value != 10 {
		t.Fatalf("Get(1) = %d, %t", value, found)
	}
	cache.Set(3, 30)
	if cache.Len() != 2 || !cache.Contains(1) || !cache.Contains(3) || cache.Contains(2) {
		t.Fatalf("unexpected ARC state: keys=%v", cache.Keys())
	}
	if value, found := cache.Peek(3); !found || value != 30 {
		t.Fatalf("Peek(3) = %d, %t", value, found)
	}
	if len(cache.Keys()) != 2 || len(cache.Values()) != 2 {
		t.Fatal("Keys or Values length does not match Len")
	}
	cache.Delete(1)
	if cache.Contains(1) {
		t.Fatal("Delete did not remove key")
	}
	cache.Purge()
	if cache.Len() != 0 {
		t.Fatalf("Len() after Purge = %d", cache.Len())
	}
}

func TestTTLAndJitterApplyToEveryWritePath(t *testing.T) {
	base := time.Unix(100, 0)
	clock := newTestClock(base)
	cache := newTestCache[int, int](t, "ttl", 8,
		arc.WithTTL(10*time.Second),
		arc.WithTTLJitter(5*time.Second),
		arc.WithoutPeriodicCleanup(),
		arc.WithNow(clock.Now),
		arc.WithJitter(func(time.Duration) time.Duration { return 3 * time.Second }),
	)

	cache.Set(1, 1)
	if _, err := cache.GetOrLoad(
		context.Background(),
		2,
		func(context.Context) (int, error) { return 2, nil },
	); err != nil {
		t.Fatal(err)
	}
	if _, err := cache.GetOrLoadMany(
		context.Background(),
		[]int{3},
		func(context.Context, []int) (map[int]int, error) {
			return map[int]int{3: 3}, nil
		},
	); err != nil {
		t.Fatal(err)
	}
	for _, key := range []int{1, 2, 3} {
		expiresAt, found := cache.PeekEntryExpiresAt(key)
		if !found || !expiresAt.Equal(base.Add(13*time.Second)) {
			t.Fatalf("key %d expiration = %v, found=%t", key, expiresAt, found)
		}
	}

	clock.Set(base.Add(13 * time.Second))
	if _, found := cache.Get(1); found || cache.Contains(2) {
		t.Fatal("expired values remained visible")
	}

	immortalClock := newTestClock(base)
	immortal := newTestCache[int, int](t, "immortal", 1,
		arc.WithoutExpiration(),
		arc.WithoutPeriodicCleanup(),
		arc.WithNow(immortalClock.Now),
		arc.WithJitter(func(time.Duration) time.Duration { panic("jitter should be disabled") }),
	)
	immortal.Set(1, 7)
	immortalClock.Set(base.Add(100 * time.Hour))
	if value, found := immortal.Get(1); !found || value != 7 {
		t.Fatalf("non-expiring Get() = %d, %t", value, found)
	}

	maximumTTL := time.Duration(1<<63 - 1)
	longLived := newTestCache[int, int](t, "long-lived", 1,
		arc.WithTTL(maximumTTL),
		arc.WithTTLJitter(2*time.Nanosecond),
		arc.WithoutPeriodicCleanup(),
		arc.WithNow(clock.Now),
		arc.WithJitter(func(time.Duration) time.Duration { return time.Nanosecond }),
	)
	longLived.Set(1, 1)
	expiresAt, found := longLived.PeekEntryExpiresAt(1)
	wantExpiration := clock.Now().Add(maximumTTL).Add(time.Nanosecond)
	if !found || !expiresAt.Equal(wantExpiration) {
		t.Fatalf("large TTL expiration = %v, want %v", expiresAt, wantExpiration)
	}
}

func TestCleanupLifecycleAndMetrics(t *testing.T) {
	metrics := newMetricRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	clock := newTestClock(time.Unix(200, 0))
	cache, err := arc.New[int, int](ctx, "cleanup", 4,
		arc.WithTTL(time.Second),
		arc.WithCleanupInterval(time.Millisecond),
		arc.WithoutTTLJitter(),
		arc.WithMetrics(metrics),
		arc.WithNow(clock.Now),
	)
	if err != nil {
		t.Fatal(err)
	}
	cache.Set(1, 1)
	cache.Set(2, 2)
	clock.Set(time.Unix(202, 0))
	eventually(t, time.Second, func() bool {
		return cache.Len() == 0 &&
			metrics.count("cache.cleanup.expired") == 2 &&
			metrics.gauge("cache.cleanup.len") == 0
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
	cache := newTestCache[int, int](t, "metrics", 2,
		arc.WithTTL(time.Second),
		arc.WithoutPeriodicCleanup(),
		arc.WithoutTTLJitter(),
		arc.WithMetrics(metrics),
		arc.WithNow(clock.Now),
	)
	cache.Set(1, 1)
	cache.Get(1)
	cache.Get(2)
	clock.Set(base.Add(2 * time.Second))
	cache.Get(1)
	cache.Set(2, 2)
	cache.Delete(2)
	cache.Purge()

	wants := map[string]int{
		"cache.metrics.hit":         1,
		"cache.metrics.miss":        1,
		"cache.metrics.expired_get": 1,
		"cache.metrics.delete":      1,
	}
	for name, want := range wants {
		if got := metrics.count(name); got != want {
			t.Errorf("metric %s = %d, want %d", name, got, want)
		}
	}
	if got := metrics.gauge("cache.metrics.len"); got != 0 {
		t.Errorf("len gauge = %v, want 0", got)
	}
}

func newTestCache[K comparable, V any](
	t *testing.T,
	name string,
	capacity int,
	options ...arc.Option,
) *arc.Cache[K, V] {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cache, err := arc.New[K, V](ctx, name, capacity, options...)
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
