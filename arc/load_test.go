package arc

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGetOrLoadValuesErrorsPanicsAndCancellation(t *testing.T) {
	cache := newTestCache[int, int](t, "load", 8, WithoutExpiration(), WithoutPeriodicCleanup())
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
	if cache.Contains(2) {
		t.Fatal("failed value was cached")
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
	if cache.Contains(4) {
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

	pointers := newTestCache[int, *int](t, "nil-value", 2, WithoutExpiration(), WithoutPeriodicCleanup())
	value, err := pointers.GetOrLoad(context.Background(), 1, func(context.Context) (*int, error) { return nil, nil })
	if err != nil || value != nil {
		t.Fatalf("nil pointer load = %v, %v", value, err)
	}
	if cached, found := pointers.Get(1); !found || cached != nil {
		t.Fatalf("cached nil pointer = %v, %t", cached, found)
	}
}

func TestGetOrLoadCoalescesSameKeyAndRunsDifferentKeysInParallel(t *testing.T) {
	cache := newTestCache[int, int](t, "coalesce", 16, WithoutExpiration(), WithoutPeriodicCleanup())
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
	close(release)
	for range callers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("same-key loader calls = %d, want 1", calls.Load())
	}

	parallel := newTestCache[int, int](t, "parallel", 4, WithoutExpiration(), WithoutPeriodicCleanup())
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

func TestGetOrLoadWaiterCanCancel(t *testing.T) {
	cache := newTestCache[int, int](t, "wait-cancel", 2, WithoutExpiration(), WithoutPeriodicCleanup())
	started := make(chan struct{})
	release := make(chan struct{})
	leaderDone := make(chan error, 1)
	go func() {
		_, err := cache.GetOrLoad(context.Background(), 1, func(context.Context) (int, error) {
			close(started)
			<-release
			return 1, nil
		})
		leaderDone <- err
	}()
	<-started
	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := cache.GetOrLoad(waitCtx, 1, func(context.Context) (int, error) { return 2, nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter error = %v", err)
	}
	close(release)
	if err := <-leaderDone; err != nil {
		t.Fatal(err)
	}
}

func TestGetOrLoadManyDeduplicatesAndFiltersLoaderResults(t *testing.T) {
	cache := newTestCache[int, int](t, "many", 8, WithoutExpiration(), WithoutPeriodicCleanup())
	cache.Set(1, 10)
	var received []int
	values, err := cache.GetOrLoadMany(context.Background(), []int{1, 2, 2, 3}, func(_ context.Context, keys []int) (map[int]int, error) {
		received = slices.Clone(keys)
		return map[int]int{2: 20, 4: 40}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(received, []int{2, 3}) {
		t.Fatalf("loader keys = %v", received)
	}
	if len(values) != 2 || values[1] != 10 || values[2] != 20 {
		t.Fatalf("values = %v", values)
	}
	if cache.Contains(3) || cache.Contains(4) {
		t.Fatal("omitted or unexpected loader keys were cached")
	}
	empty, err := cache.GetOrLoadMany(context.Background(), nil, func(context.Context, []int) (map[int]int, error) {
		t.Fatal("empty load invoked loader")
		return nil, nil
	})
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty load = %v, %v", empty, err)
	}
}

func TestGetOrLoadManyCoalescesOverlappingBatches(t *testing.T) {
	cache := newTestCache[int, int](t, "overlap", 8, WithoutExpiration(), WithoutPeriodicCleanup())
	aStarted := make(chan struct{})
	releaseA := make(chan struct{})
	type outcome struct {
		values map[int]int
		err    error
	}
	aResult := make(chan outcome, 1)
	go func() {
		values, err := cache.GetOrLoadMany(context.Background(), []int{1, 2}, func(_ context.Context, keys []int) (map[int]int, error) {
			if !slices.Equal(keys, []int{1, 2}) {
				return nil, errors.New("unexpected first batch keys")
			}
			close(aStarted)
			<-releaseA
			return map[int]int{1: 10, 2: 20}, nil
		})
		aResult <- outcome{values: values, err: err}
	}()
	<-aStarted

	bKeys := make(chan []int, 1)
	bResult := make(chan outcome, 1)
	go func() {
		values, err := cache.GetOrLoadMany(context.Background(), []int{2, 3}, func(_ context.Context, keys []int) (map[int]int, error) {
			bKeys <- slices.Clone(keys)
			return map[int]int{3: 30}, nil
		})
		bResult <- outcome{values: values, err: err}
	}()
	select {
	case keys := <-bKeys:
		if !slices.Equal(keys, []int{3}) {
			t.Fatalf("second batch loader keys = %v", keys)
		}
	case <-time.After(time.Second):
		t.Fatal("second batch loader did not start")
	}
	select {
	case result := <-bResult:
		t.Fatalf("second batch completed before overlap owner: %#v", result)
	case <-time.After(10 * time.Millisecond):
	}
	if cache.Contains(3) {
		t.Fatal("staged batch value was published before joined key completed")
	}

	close(releaseA)
	first := <-aResult
	second := <-bResult
	if first.err != nil || second.err != nil {
		t.Fatalf("batch errors = %v, %v", first.err, second.err)
	}
	if second.values[2] != 20 || second.values[3] != 30 {
		t.Fatalf("second batch values = %v", second.values)
	}
}

func TestGetOrLoadManyDoesNotPublishStagedValuesAfterJoinedError(t *testing.T) {
	cache := newTestCache[int, int](t, "overlap-error", 8, WithoutExpiration(), WithoutPeriodicCleanup())
	wantErr := errors.New("shared load failed")
	ownerStarted := make(chan struct{})
	releaseOwner := make(chan struct{})
	ownerResult := make(chan error, 1)
	go func() {
		_, err := cache.GetOrLoadMany(context.Background(), []int{2}, func(context.Context, []int) (map[int]int, error) {
			close(ownerStarted)
			<-releaseOwner
			return nil, wantErr
		})
		ownerResult <- err
	}()
	<-ownerStarted

	staged := make(chan struct{})
	joinedResult := make(chan error, 1)
	go func() {
		_, err := cache.GetOrLoadMany(context.Background(), []int{2, 3}, func(_ context.Context, keys []int) (map[int]int, error) {
			if !slices.Equal(keys, []int{3}) {
				return nil, errors.New("unexpected staged keys")
			}
			close(staged)
			return map[int]int{3: 30}, nil
		})
		joinedResult <- err
	}()
	<-staged
	close(releaseOwner)
	if err := <-ownerResult; !errors.Is(err, wantErr) {
		t.Fatalf("owner error = %v", err)
	}
	if err := <-joinedResult; !errors.Is(err, wantErr) {
		t.Fatalf("joined error = %v", err)
	}
	if cache.Contains(3) {
		t.Fatal("staged value was cached after joined error")
	}
}

func TestSingleLoadJoinsBatchByComparableKey(t *testing.T) {
	type key struct{ ID int }
	cache := newTestCache[key, int](t, "cross", 4, WithoutExpiration(), WithoutPeriodicCleanup())
	started := make(chan struct{})
	release := make(chan struct{})
	batchDone := make(chan error, 1)
	go func() {
		_, err := cache.GetOrLoadMany(context.Background(), []key{{ID: 1}}, func(context.Context, []key) (map[key]int, error) {
			close(started)
			<-release
			return map[key]int{{ID: 1}: 10}, nil
		})
		batchDone <- err
	}()
	<-started
	var singleCalls atomic.Int32
	singleDone := make(chan error, 1)
	go func() {
		value, err := cache.GetOrLoad(context.Background(), key{ID: 1}, func(context.Context) (int, error) {
			singleCalls.Add(1)
			return 20, nil
		})
		if err == nil && value != 10 {
			err = errors.New("single caller received wrong batch value")
		}
		singleDone <- err
	}()
	close(release)
	if err := <-batchDone; err != nil {
		t.Fatal(err)
	}
	if err := <-singleDone; err != nil {
		t.Fatal(err)
	}
	if singleCalls.Load() != 0 {
		t.Fatalf("single loader calls = %d, want 0", singleCalls.Load())
	}
}

func TestGetOrLoadManyValidationErrorsAndCancellation(t *testing.T) {
	cache := newTestCache[int, int](t, "many-errors", 4, WithoutExpiration(), WithoutPeriodicCleanup())
	if _, err := cache.GetOrLoadMany(nil, []int{1}, func(context.Context, []int) (map[int]int, error) { //nolint:staticcheck // Verifies the explicit nil-context contract.
		return nil, nil
	}); !errors.Is(err, ErrNilContext) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := cache.GetOrLoadMany(context.Background(), []int{1}, nil); !errors.Is(err, ErrNilLoader) {
		t.Fatalf("nil loader error = %v", err)
	}
	if _, err := cache.GetOrLoadMany(context.Background(), []int{1}, func(context.Context, []int) (map[int]int, error) { panic("boom") }); !errors.Is(err, ErrLoaderPanic) {
		t.Fatalf("panic error = %v", err)
	}
	wantErr := errors.New("batch failed")
	if _, err := cache.GetOrLoadMany(context.Background(), []int{2}, func(context.Context, []int) (map[int]int, error) {
		return map[int]int{2: 2}, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("loader error = %v", err)
	}
	if cache.Contains(2) {
		t.Fatal("partial error result was cached")
	}
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := cache.GetOrLoadMany(ctx, []int{3}, func(context.Context, []int) (map[int]int, error) {
		cancel()
		return map[int]int{3: 3}, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled batch error = %v", err)
	}
	if cache.Contains(3) {
		t.Fatal("canceled batch result was cached")
	}
}

func TestConcurrentOperations(t *testing.T) {
	cache := newTestCache[int, int](t, "race", 64, WithoutExpiration(), WithoutPeriodicCleanup())
	var wait sync.WaitGroup
	for worker := range 12 {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for index := range 2_000 {
				key := (worker + index) % 128
				cache.Set(key, index)
				cache.Get(key)
				cache.Peek(key)
				cache.Contains(key)
				if index%17 == 0 {
					cache.Delete(key)
				}
			}
		}(worker)
	}
	wait.Wait()
	if cache.Len() > cache.Capacity() {
		t.Fatalf("Len() = %d exceeds capacity %d", cache.Len(), cache.Capacity())
	}
}
