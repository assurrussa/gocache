package simple_test

import (
	"context"
	"fmt"

	"github.com/assurrussa/gocache/simple"
)

func ExampleCache_GetOrLoad() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, err := simple.New[string, int](ctx, "scores")
	if err != nil {
		panic(err)
	}
	value, err := cache.GetOrLoad(ctx, "alice", func(context.Context) (int, error) {
		return 42, nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
	// Output: 42
}
