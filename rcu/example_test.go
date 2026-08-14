package rcu_test

import (
	"context"
	"fmt"

	"github.com/assurrussa/gocache/rcu"
)

func ExampleCache() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cache, err := rcu.New(ctx, func(context.Context) (map[string]int, error) {
		return map[string]int{"active": 3}, nil
	}, rcu.WithoutPeriodicRefresh())
	if err != nil {
		panic(err)
	}
	if err := cache.WaitInitial(ctx); err != nil {
		panic(err)
	}

	active, found := cache.Get("active")
	fmt.Println(active, found)
	// Output: 3 true
}
