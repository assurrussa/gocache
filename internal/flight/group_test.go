package flight

import (
	"context"
	"errors"
	"testing"
)

func TestGroupClaimsAndSharesResults(t *testing.T) {
	var group Group[int, string]
	claims := group.ClaimMany([]int{1, 2})
	if !claims[0].Owned() || !claims[1].Owned() {
		t.Fatal("first claims must own distinct keys")
	}
	waiter := group.Claim(1)
	if waiter.Owned() {
		t.Fatal("second claim for the same key must wait")
	}

	claims[0].Complete(Result[string]{Value: "one", Found: true})
	result, err := waiter.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Found || result.Value != "one" {
		t.Fatalf("Wait() = %#v, want shared value", result)
	}

	// Duplicate completion is intentionally harmless.
	claims[0].Complete(Result[string]{Err: errors.New("late")})
	fresh := group.Claim(1)
	if !fresh.Owned() {
		t.Fatal("completed key must be claimable again")
	}
	fresh.Complete(Result[string]{})
	claims[1].Complete(Result[string]{})
}

func TestClaimWaitHonorsContext(t *testing.T) {
	var group Group[int, int]
	owner := group.Claim(1)
	waiter := group.Claim(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := waiter.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want context canceled", err)
	}
	owner.Complete(Result[int]{Value: 7, Found: true})
	result, err := waiter.Wait(context.Background())
	if err != nil || result.Value != 7 {
		t.Fatalf("Wait() after completion = %#v, %v", result, err)
	}
}
