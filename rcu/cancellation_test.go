package rcu_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/assurrussa/gocache/rcu"
)

func TestRefreshCancellationWhileWaitingForLoader(t *testing.T) {
	workerCtx, stopWorker := context.WithCancel(context.Background())
	defer stopWorker()
	entered := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var calls atomic.Int32
	cache, err := rcu.New(workerCtx, func(context.Context) (map[string]int, error) {
		call := calls.Add(1)
		if call == 1 {
			close(entered)
			<-release
		}
		return map[string]int{callsKey: int(call)}, nil
	}, rcu.WithoutPeriodicRefresh())
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		result <- cache.Refresh(ctx)
	}()
	<-started
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Refresh() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled Refresh is still waiting for another caller's loader")
	}
	if calls.Load() != 1 || cache.Len() != 0 {
		t.Fatal("canceled waiter invoked the loader or published a snapshot")
	}
	unblock()
	if err := cache.WaitInitial(workerCtx); err != nil {
		t.Fatal(err)
	}
	if err := cache.Refresh(workerCtx); err != nil {
		t.Fatal(err)
	}
	if value, found := cache.Get(callsKey); !found || value != 2 || calls.Load() != 2 {
		t.Fatalf("later Refresh did not publish: value=%d, found=%v, calls=%d", value, found, calls.Load())
	}
}

func TestWorkerStopsWhileExplicitRefreshIsRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		workerCtx, stopWorker := context.WithCancel(context.Background())
		defer stopWorker()
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		explicitDone := make(chan struct{})
		initial := true
		cache, err := rcu.New(workerCtx, func(context.Context) (map[string]int, error) {
			if initial {
				initial = false
				return map[string]int{callsKey: 1}, nil
			}
			<-release
			return map[string]int{callsKey: 2}, nil
		}, rcu.WithoutPeriodicRefresh())
		if err != nil {
			t.Fatal(err)
		}
		if err := cache.WaitInitial(workerCtx); err != nil {
			t.Fatal(err)
		}
		go func() {
			defer close(explicitDone)
			if err := cache.Refresh(context.Background()); err != nil {
				t.Errorf("explicit Refresh() error = %v", err)
			}
		}()
		synctest.Wait()
		cache.Notify()
		synctest.Wait()
		stopWorker()
		synctest.Wait()
		select {
		case <-cache.Done():
		default:
			t.Fatal("worker shutdown is waiting for an independent explicit refresh")
		}
		select {
		case <-explicitDone:
			t.Fatal("worker cancellation stopped the independent explicit refresh")
		default:
		}
		unblock()
		<-explicitDone
		if value, found := cache.Get(callsKey); !found || value != 2 {
			t.Fatalf("explicit refresh did not publish after worker shutdown: value=%d, found=%v", value, found)
		}
	})
}
