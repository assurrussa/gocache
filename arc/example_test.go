package arc_test

import (
	"context"
	"fmt"

	"github.com/assurrussa/gocache/arc"
)

func ExampleCache_GetOrLoad() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, err := arc.New[string, int](ctx, "scores", 128, arc.WithoutPeriodicCleanup())
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
