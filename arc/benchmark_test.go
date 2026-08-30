package arc

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkGet(b *testing.B) {
	cache := benchmarkCache[string, int](b, 1_024)
	cache.Set("key", 42)
	b.ReportAllocs()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			_, _ = cache.Get("key")
		}
	})
}

func BenchmarkSet(b *testing.B) {
	cache := benchmarkCache[string, int](b, 1_024)
	b.ReportAllocs()
	for index := range b.N {
		cache.Set(strconv.Itoa(index%2_048), index)
	}
}

func BenchmarkGetOrLoad(b *testing.B) {
	cache := benchmarkCache[string, int](b, 1)
	cache.Set("key", 42)
	b.ReportAllocs()
	for range b.N {
		_, err := cache.GetOrLoad(context.Background(), "key", func(context.Context) (int, error) { return 0, nil })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetOrLoadMany(b *testing.B) {
	for _, size := range []int{1, 100} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			cache := benchmarkCache[int, int](b, size)
			keys := make([]int, size)
			for index := range keys {
				keys[index] = index
				cache.Set(index, index)
			}
			b.ReportAllocs()
			for range b.N {
				_, err := cache.GetOrLoadMany(context.Background(), keys, func(context.Context, []int) (map[int]int, error) { return nil, nil })
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkCache[K comparable, V any](b *testing.B, capacity int) *Cache[K, V] {
	b.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)
	cache, err := New[K, V](ctx, "benchmark", capacity, WithoutExpiration(), WithoutPeriodicCleanup())
	if err != nil {
		b.Fatal(err)
	}
	return cache
}
