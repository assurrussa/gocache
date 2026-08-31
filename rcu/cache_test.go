package rcu_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/assurrussa/gocache/rcu"
)

const callsKey = "calls"

func TestNewValidatesInputsAndOptions(t *testing.T) {
	loader := func(context.Context) (map[string]int, error) {
		return map[string]int{}, nil
	}
	tests := []struct {
		name    string
		nilCtx  bool
		loader  rcu.Loader[string, int]
		options []rcu.Option
		wantErr error
	}{
		{name: "nil context", nilCtx: true, loader: loader, wantErr: rcu.ErrNilContext},
		{name: "nil loader", wantErr: rcu.ErrNilLoader},
		{name: "nil option", loader: loader, options: []rcu.Option{nil}, wantErr: rcu.ErrNilOption},
		{
			name:    "zero interval",
			loader:  loader,
			options: []rcu.Option{rcu.WithRefreshInterval(0)},
			wantErr: rcu.ErrInvalidRefreshInterval,
		},
		{
			name:    "negative interval",
			loader:  loader,
			options: []rcu.Option{rcu.WithRefreshInterval(-time.Second)},
			wantErr: rcu.ErrInvalidRefreshInterval,
		},
		{
			name:    "nil error handler",
			loader:  loader,
			options: []rcu.Option{rcu.WithErrorHandler(nil)},
			wantErr: rcu.ErrNilErrorHandler,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			if test.nilCtx {
				ctx = nil
			}
			cache, err := rcu.New(ctx, test.loader, test.options...)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("New() error = %v, want %v", err, test.wantErr)
			}
			if cache != nil {
				t.Fatal("New() returned a cache with invalid input")
			}
		})
	}
}

func TestInitialLoadPublishesCopiedSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := map[string]int{"active": 2, "done": 4}
	cache, err := rcu.New(ctx, func(context.Context) (map[string]int, error) {
		return source, nil
	}, rcu.WithoutPeriodicRefresh())
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.WaitInitial(ctx); err != nil {
		t.Fatalf("WaitInitial() error = %v", err)
	}
	if cache.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", cache.Len())
	}

	source["active"] = 99
	delete(source, "done")
	if value, found := cache.Get("active"); !found || value != 2 {
		t.Fatalf("Get(active) = %d, %t", value, found)
	}
	if value, found := cache.Get("done"); !found || value != 4 {
		t.Fatalf("Get(done) = %d, %t", value, found)
	}

	snapshot := cache.Snapshot()
	snapshot["active"] = 100
	if value, _ := cache.Get("active"); value != 2 {
		t.Fatalf("mutating Snapshot changed cache value to %d", value)
	}
}

func TestFailedRefreshPreservesLastGoodSnapshot(t *testing.T) {
	wantErr := errors.New("source unavailable")
	var fail atomic.Bool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, err := rcu.New(ctx, func(context.Context) (map[string]int, error) {
		if fail.Load() {
			return nil, wantErr
		}
		return map[string]int{"ready": 7}, nil
	}, rcu.WithoutPeriodicRefresh())
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.WaitInitial(ctx); err != nil {
		t.Fatal(err)
	}

	fail.Store(true)
	if err := cache.Refresh(ctx); !errors.Is(err, wantErr) {
		t.Fatalf("Refresh() error = %v, want %v", err, wantErr)
	}
	if value, found := cache.Get("ready"); !found || value != 7 {
		t.Fatalf("Get(ready) after failed refresh = %d, %t", value, found)
	}
}

func TestRefreshSerializesLoaderCalls(t *testing.T) {
	var concurrent atomic.Int32
	var maximum atomic.Int32
	loader := func(context.Context) (map[int]int, error) {
		current := concurrent.Add(1)
		defer concurrent.Add(-1)
		for {
			seen := maximum.Load()
			if current <= seen || maximum.CompareAndSwap(seen, current) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		return map[int]int{1: 1}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, err := rcu.New(ctx, loader, rcu.WithoutPeriodicRefresh())
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.WaitInitial(ctx); err != nil {
		t.Fatal(err)
	}

	var wait sync.WaitGroup
	for range 24 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := cache.Refresh(ctx); err != nil {
				t.Errorf("Refresh() error = %v", err)
			}
		}()
	}
	wait.Wait()
	if maximum.Load() != 1 {
		t.Fatalf("maximum concurrent loaders = %d, want 1", maximum.Load())
	}
}

func TestNotifyCoalescesBurstWhileRefreshRuns(t *testing.T) {
	var calls atomic.Int32
	secondStarted := make(chan struct{})
	releaseSecond := make(chan struct{})
	thirdFinished := make(chan struct{})
	loader := func(context.Context) (map[string]int, error) {
		call := calls.Add(1)
		switch call {
		case 2:
			close(secondStarted)
			<-releaseSecond
		case 3:
			close(thirdFinished)
		}
		return map[string]int{callsKey: int(call)}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, err := rcu.New(ctx, loader, rcu.WithoutPeriodicRefresh())
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.WaitInitial(ctx); err != nil {
		t.Fatal(err)
	}

	cache.Notify()
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("notified refresh did not start")
	}
	for range 100 {
		cache.Notify()
	}
	close(releaseSecond)
	select {
	case <-thirdFinished:
	case <-time.After(time.Second):
		t.Fatal("coalesced follow-up refresh did not run")
	}
	time.Sleep(10 * time.Millisecond)
	if calls.Load() != 3 {
		t.Fatalf("loader calls = %d, want exactly 3", calls.Load())
	}
}

func TestPeriodicRefreshAndContextBoundWorker(t *testing.T) {
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	cache, err := rcu.New(ctx, func(context.Context) (map[string]int, error) {
		call := calls.Add(1)
		return map[string]int{callsKey: int(call)}, nil
	}, rcu.WithRefreshInterval(5*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.WaitInitial(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if calls.Load() < 2 {
		t.Fatal("periodic refresh did not run")
	}

	cancel()
	select {
	case <-cache.Done():
	case <-time.After(time.Second):
		t.Fatal("background worker did not stop after cancellation")
	}
	stoppedAt := calls.Load()
	cache.Notify()
	time.Sleep(15 * time.Millisecond)
	if calls.Load() != stoppedAt {
		t.Fatalf("loader ran after worker stopped: before=%d after=%d", stoppedAt, calls.Load())
	}
}

func TestBackgroundErrorHandlerAndWaitInitial(t *testing.T) {
	wantErr := errors.New("initial failure")
	errorsCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, err := rcu.New(ctx, func(context.Context) (map[string]int, error) {
		return nil, wantErr
	}, rcu.WithoutPeriodicRefresh(), rcu.WithErrorHandler(func(_ context.Context, err error) {
		errorsCh <- err
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.WaitInitial(ctx); !errors.Is(err, wantErr) {
		t.Fatalf("WaitInitial() error = %v, want %v", err, wantErr)
	}
	select {
	case err := <-errorsCh:
		if !errors.Is(err, wantErr) {
			t.Fatalf("error handler received %v, want %v", err, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("error handler was not called")
	}
}

func TestWaitInitialHonorsCallerContext(t *testing.T) {
	loaderRelease := make(chan struct{})
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	defer cancelWorker()
	cache, err := rcu.New(workerCtx, func(context.Context) (map[string]int, error) {
		<-loaderRelease
		return map[string]int{}, nil
	}, rcu.WithoutPeriodicRefresh())
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	if err := cache.WaitInitial(waitCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitInitial() error = %v, want context canceled", err)
	}
	close(loaderRelease)
	if err := cache.WaitInitial(context.Background()); err != nil {
		t.Fatalf("WaitInitial() after release error = %v", err)
	}
}

func TestWaitInitialSupportsMultipleCallers(t *testing.T) {
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, err := rcu.New(ctx, func(context.Context) (map[string]int, error) {
		<-release
		return map[string]int{"ready": 1}, nil
	}, rcu.WithoutPeriodicRefresh())
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 8)
	for range cap(results) {
		go func() {
			results <- cache.WaitInitial(ctx)
		}()
	}
	close(release)
	for range cap(results) {
		if err := <-results; err != nil {
			t.Fatalf("WaitInitial() error = %v", err)
		}
	}
}

func TestExplicitRefreshRemainsAvailableAfterWorkerStops(t *testing.T) {
	var calls atomic.Int32
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	cache, err := rcu.New(workerCtx, func(context.Context) (map[string]int, error) {
		call := calls.Add(1)
		return map[string]int{callsKey: int(call)}, nil
	}, rcu.WithoutPeriodicRefresh())
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.WaitInitial(workerCtx); err != nil {
		t.Fatal(err)
	}
	cancelWorker()
	select {
	case <-cache.Done():
	case <-time.After(time.Second):
		t.Fatal("background worker did not stop")
	}
	if err := cache.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() after worker stop error = %v", err)
	}
	if value, _ := cache.Get(callsKey); value != 2 {
		t.Fatalf("refreshed value = %d, want 2", value)
	}
}

func TestNilCacheReadMethodsAreSafe(t *testing.T) {
	var cache *rcu.Cache[string, int]
	if value, found := cache.Get("missing"); found || value != 0 {
		t.Fatalf("Get() = %d, %t", value, found)
	}
	if cache.Len() != 0 || len(cache.Snapshot()) != 0 {
		t.Fatal("nil cache returned data")
	}
	if err := cache.Refresh(context.Background()); !errors.Is(err, rcu.ErrNilCache) {
		t.Fatalf("Refresh() error = %v, want ErrNilCache", err)
	}
	if err := cache.WaitInitial(context.Background()); !errors.Is(err, rcu.ErrNilCache) {
		t.Fatalf("WaitInitial() error = %v, want ErrNilCache", err)
	}
	cache.Notify()
}
