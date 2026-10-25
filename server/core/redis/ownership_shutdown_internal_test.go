// Copyright 2026 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
	"github.com/runatlantis/atlantis/server/logging"
	"github.com/stretchr/testify/require"
)

func TestOwnerStore_AbandonedClaimCannotBeReadmitted(t *testing.T) {
	mr := miniredis.RunT(t)
	store := newTestOwnerStoreWithTTL(t, mr, "atlantis-0", testOwnerURL, 30*time.Second)
	key := testOwnershipKey(101)
	claim, err := store.Claim(context.Background(), key)
	require.NoError(t, err)
	require.NoError(t, store.Abandon())
	mr.FastForward(20 * time.Second)
	admitted, err := store.Admit(context.Background(), key, claim.ClaimID)
	require.NoError(t, err)
	require.False(t, admitted, "shutdown must stop per-command admission from extending abandoned leases")
	mr.FastForward(11 * time.Second)
	_, found, err := store.Current(context.Background(), key)
	require.NoError(t, err)
	require.False(t, found, "abandoned claim must expire at its original deadline")
}

func TestOwnerStore_DrainAllowsExistingCommandAdmission(t *testing.T) {
	mr := miniredis.RunT(t)
	store := newTestOwnerStoreWithTTL(t, mr, "atlantis-0", testOwnerURL, 30*time.Second)
	key := testOwnershipKey(102)
	claim, err := store.Claim(context.Background(), key)
	require.NoError(t, err)
	store.BeginDrain()
	mr.FastForward(20 * time.Second)
	admitted, err := store.Admit(context.Background(), key, claim.ClaimID)
	require.NoError(t, err)
	require.True(t, admitted, "in-flight work may continue during the grace period")
	mr.FastForward(11 * time.Second)
	_, found, err := store.Current(context.Background(), key)
	require.NoError(t, err)
	require.True(t, found)
}

type blockedAdmissionClient struct {
	redislib.Cmdable
	started        chan struct{}
	release        chan struct{}
	cancelObserved chan struct{}
	cancelReturn   chan struct{}
}

func (c *blockedAdmissionClient) Eval(ctx context.Context, script string, keys []string, args ...any) *redislib.Cmd {
	if script == renewOwnerScript {
		close(c.started)
		select {
		case <-c.release:
		case <-ctx.Done():
			if c.cancelObserved != nil {
				close(c.cancelObserved)
				<-c.cancelReturn
			}
			result := redislib.NewCmd(ctx)
			result.SetErr(ctx.Err())
			return result
		}
	}
	return c.Cmdable.Eval(ctx, script, keys, args...)
}

func TestOwnerStore_AbandonWaitsForCancelledAdmission(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: mr.Addr()})
	defer client.Close()
	blocked := &blockedAdmissionClient{Cmdable: client, started: make(chan struct{}), release: make(chan struct{}), cancelObserved: make(chan struct{}), cancelReturn: make(chan struct{})}
	store, err := NewOwnerStoreWithClient(blocked, OwnerStoreConfig{ReplicaID: "atlantis-0", AdvertiseURL: testOwnerURL, TTL: 30 * time.Second}, logging.NewNoopLogger(t))
	require.NoError(t, err)
	defer store.Close()
	defer close(blocked.release)
	// Allow cleanup to complete even if the assertion fails.
	defer func() {
		select {
		case <-blocked.cancelReturn:
		default:
			close(blocked.cancelReturn)
		}
	}()
	key := testOwnershipKey(103)
	claim, err := store.Claim(context.Background(), key)
	require.NoError(t, err)
	admitted := make(chan error, 1)
	go func() { _, err := store.Admit(context.Background(), key, claim.ClaimID); admitted <- err }()
	<-blocked.started
	stopped := make(chan error, 1)
	go func() { stopped <- store.Abandon() }()
	select {
	case <-blocked.cancelObserved:
	case <-time.After(time.Second):
		t.Fatal("abandon did not cancel synchronous renewal")
	}
	select {
	case <-stopped:
		t.Fatal("abandon returned before cancelled renewal exited")
	default:
	}
	close(blocked.cancelReturn)
	require.ErrorIs(t, <-admitted, context.Canceled)
	require.NoError(t, <-stopped)
	allowed, err := store.Admit(context.Background(), key, claim.ClaimID)
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestOwnerStore_AbandonCancelsBlockedAdmission(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: mr.Addr()})
	defer client.Close()
	blocked := &blockedAdmissionClient{Cmdable: client, started: make(chan struct{}), release: make(chan struct{})}
	store, err := NewOwnerStoreWithClient(blocked, OwnerStoreConfig{ReplicaID: "atlantis-0", AdvertiseURL: testOwnerURL, TTL: time.Minute}, logging.NewNoopLogger(t))
	require.NoError(t, err)
	defer store.Close()
	// Always release the test transport so a failing assertion cannot hang cleanup.
	defer close(blocked.release)
	key := testOwnershipKey(104)
	claim, err := store.Claim(context.Background(), key)
	require.NoError(t, err)
	admitted := make(chan error, 1)
	go func() { _, err := store.Admit(context.Background(), key, claim.ClaimID); admitted <- err }()
	<-blocked.started
	stopped := make(chan error, 1)
	go func() { stopped <- store.Abandon() }()
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("abandon must cancel blocked admission instead of waiting for the caller's deadline")
	}
	require.ErrorIs(t, <-admitted, context.Canceled)
	mr.FastForward(61 * time.Second)
	_, found, err := store.Current(context.Background(), key)
	require.NoError(t, err)
	require.False(t, found, "cancelled admission must not renew after abandon returns")
}
