package astore

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// counter is a test loader whose value, failure and delay are controllable.
type counter struct {
	calls   atomic.Int64
	failFor atomic.Int64 // fail the first n calls
	value   atomic.Int64
	delay   time.Duration
}

func (c *counter) load(ctx context.Context) (int, error) {
	c.calls.Add(1)
	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if c.failFor.Add(-1) >= 0 {
		return 0, errors.New("backend down")
	}
	return int(c.value.Load()), nil
}

func TestStore_StartLoadsImmediately(t *testing.T) {
	c := &counter{}
	c.value.Store(7)
	s := New(Options{Name: "t"}, c.load)

	require.NoError(t, s.Start(context.Background()))
	gen, ok := s.Current()
	require.True(t, ok)
	assert.Equal(t, 7, gen.Value)
	assert.False(t, gen.LoadedAt.IsZero())
}

func TestStore_PeriodicRefreshWarmsWithoutReads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &counter{}
	s := New(Options{Name: "t", Interval: 20 * time.Millisecond, Jitter: -1}, c.load)
	require.NoError(t, s.Start(ctx))
	first, _ := s.Current()

	c.value.Store(9)
	require.Eventually(t, func() bool {
		gen, ok := s.Current()
		return ok && gen.Value == 9 && gen.LoadedAt.After(first.LoadedAt)
	}, 2*time.Second, 5*time.Millisecond, "the loop must refresh without a reader")
	assert.Greater(t, c.calls.Load(), int64(1))
}

func TestStore_LazyTTLRefreshesOnRead(t *testing.T) {
	c := &counter{}
	s := New(Options{Name: "t", TTL: 10 * time.Millisecond, Jitter: -1}, c.load)
	require.NoError(t, s.Refresh(context.Background()))

	c.value.Store(3)
	// Within the TTL the value is served as-is.
	gen, ok := s.Load(context.Background())
	require.True(t, ok)
	assert.Equal(t, 0, gen.Value)

	time.Sleep(15 * time.Millisecond)
	gen, ok = s.Load(context.Background())
	require.True(t, ok)
	assert.Equal(t, 0, gen.Value, "a stale read serves the old generation first")
	require.Eventually(t, func() bool {
		gen, _ := s.Current()
		return gen.Value == 3
	}, 2*time.Second, 5*time.Millisecond, "the stale read must trigger a background refresh")
}

func TestStore_CachingDisabledLoadsEveryRead(t *testing.T) {
	c := &counter{}
	s := New(Options{Name: "t", Jitter: -1}, c.load)

	for i := 0; i < 3; i++ {
		s.Load(context.Background())
	}
	assert.Equal(t, int64(3), c.calls.Load())
}

func TestStore_RefreshFailureKeepsThePreviousGeneration(t *testing.T) {
	c := &counter{}
	c.value.Store(5)
	s := New(Options{Name: "t", TTL: time.Minute}, c.load)
	require.NoError(t, s.Refresh(context.Background()))

	c.failFor.Store(1)
	err := s.Refresh(context.Background())
	require.Error(t, err)

	gen, ok := s.Load(context.Background())
	require.True(t, ok)
	assert.Equal(t, 5, gen.Value, "a failed refresh must not erase the snapshot")

	loadedAt, statusErr := s.Status()
	require.Error(t, statusErr)
	assert.False(t, loadedAt.IsZero())
}

func TestStore_LoadWithNoSnapshotReportsTheError(t *testing.T) {
	c := &counter{}
	c.failFor.Store(1)
	s := New(Options{Name: "t", TTL: time.Minute}, c.load)

	_, ok := s.Load(context.Background())
	assert.False(t, ok)

	_, err := s.LoadE(context.Background())
	// The second call retries; the backend is up again, so it succeeds.
	assert.NoError(t, err)
}

func TestStore_ConcurrentRefreshesCollapse(t *testing.T) {
	c := &counter{delay: 200 * time.Millisecond}
	s := New(Options{Name: "t"}, c.load)

	const goroutines = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start
			_ = s.Refresh(context.Background())
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int64(1), c.calls.Load())
}

func TestStore_StartIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &counter{delay: 5 * time.Millisecond}
	s := New(Options{Name: "t", Interval: 10 * time.Millisecond, Jitter: -1}, c.load)

	require.NoError(t, s.Start(ctx))
	require.NoError(t, s.Start(ctx))
	assert.Equal(t, int64(1), c.calls.Load(), "the second Start must not load again")

	time.Sleep(60 * time.Millisecond)
	assert.Less(t, c.calls.Load(), int64(10), "a second Start must not start a second loop")
}

func TestStore_StartRetriesAfterAFailedFirstLoad(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := &counter{}
	c.failFor.Store(2)
	c.value.Store(11)
	s := New(Options{
		Name:         "t",
		Interval:     time.Hour, // too slow to explain the recovery
		FailureRetry: 10 * time.Millisecond,
		Jitter:       -1,
	}, c.load)

	require.Error(t, s.Start(ctx), "the first load failed, so Start reports it")
	require.Eventually(t, func() bool {
		gen, ok := s.Current()
		return ok && gen.Value == 11
	}, 2*time.Second, 5*time.Millisecond, "the loop must retry after the failed first load")
}

func TestStore_InvalidateForcesTheNextLoad(t *testing.T) {
	c := &counter{}
	c.value.Store(1)
	s := New(Options{Name: "t", TTL: time.Hour}, c.load)
	require.NoError(t, s.Refresh(context.Background()))

	s.Invalidate()
	_, ok := s.Current()
	assert.False(t, ok)

	c.value.Store(2)
	gen, ok := s.Load(context.Background())
	require.True(t, ok)
	assert.Equal(t, 2, gen.Value)
}

func TestStore_NilIsUsable(t *testing.T) {
	var s *Store[int]

	_, ok := s.Current()
	assert.False(t, ok)
	_, ok = s.Load(context.Background())
	assert.False(t, ok)
	assert.NoError(t, s.Start(context.Background()))
	assert.NoError(t, s.Refresh(context.Background()))
	s.Invalidate()
}

func TestRun_FirstCallFailureStillStartsTheLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls atomic.Int64
	var failFirst atomic.Bool
	failFirst.Store(true)

	err := Run(ctx, Options{
		Name:         "t",
		Interval:     time.Hour,
		FailureRetry: 10 * time.Millisecond,
		Jitter:       -1,
	}, func(context.Context) error {
		if failFirst.CompareAndSwap(true, false) {
			return errors.New("not ready")
		}
		calls.Add(1)
		return nil
	})
	require.Error(t, err, "the first call's error is returned")
	require.Eventually(t, func() bool { return calls.Load() >= 1 },
		2*time.Second, 5*time.Millisecond, "the loop must keep running after a failed first call")
}

func TestRun_ZeroIntervalCallsOnce(t *testing.T) {
	var calls atomic.Int64
	require.NoError(t, Run(context.Background(), Options{Name: "t"}, func(context.Context) error {
		calls.Add(1)
		return nil
	}))
	assert.Equal(t, int64(1), calls.Load())
}
