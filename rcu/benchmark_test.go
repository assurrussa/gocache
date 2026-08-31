package rcu_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/assurrussa/gocache/rcu"
)

func BenchmarkGet(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, err := rcu.New(ctx, func(context.Context) (map[string]int, error) {
		return map[string]int{"key": 42}, nil
	}, rcu.WithoutPeriodicRefresh())
	if err != nil {
		b.Fatal(err)
	}
	if err := cache.WaitInitial(ctx); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			_, _ = cache.Get("key")
		}
	})
}

func BenchmarkRefresh1000(b *testing.B) {
	source := make(map[string]int, 1_000)
	for index := range 1_000 {
		source[strconv.Itoa(index)] = index
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, err := rcu.New(ctx, func(context.Context) (map[string]int, error) {
		return source, nil
	}, rcu.WithoutPeriodicRefresh())
	if err != nil {
		b.Fatal(err)
	}
	if err := cache.WaitInitial(ctx); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := cache.Refresh(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
