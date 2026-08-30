package simple

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func BenchmarkGet(b *testing.B) {
	cache := benchmarkCache[string, int](b)
	cache.Set("key", 42)
	b.ReportAllocs()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			_, _ = cache.Get("key")
		}
	})
}

func BenchmarkSet(b *testing.B) {
	cache := benchmarkCache[string, int](b)
	b.ReportAllocs()
	for index := range b.N {
		cache.Set(strconv.Itoa(index%2_048), index)
	}
}

func BenchmarkGetOrLoad(b *testing.B) {
	cache := benchmarkCache[string, int](b)
	cache.Set("key", 42)
	b.ReportAllocs()
	for range b.N {
		_, err := cache.GetOrLoad(context.Background(), "key", func(context.Context) (int, error) { return 0, nil })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkCache[K comparable, V any](b *testing.B) *Cache[K, V] {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	cache, err := New[K, V](ctx, "benchmark", WithCleanupInterval(time.Hour))
	if err != nil {
		b.Fatal(err)
	}
	return cache
}
