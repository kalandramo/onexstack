package astore

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/singleflight"
)

// ErrNotFound reports that a lookup found no row.
//
// It exists so a Named store can tell "the row does not exist" apart from "the
// backend failed": the first is an answer, the second is a failure worth
// logging. A lookup may also report a missing row as (nil, nil); both forms are
// accepted.
var ErrNotFound = errors.New("astore: not found")

const (
	// DefaultJitter is the fraction by which a refresh interval is randomly
	// shortened or lengthened. Replicas that started together and share an
	// interval would otherwise query the backend in lockstep.
	DefaultJitter = 0.1
	// DefaultLookupTimeout bounds a Named store's per-key read-through. It is
	// short on purpose: a single row on a unique key, and the caller is waiting.
	DefaultLookupTimeout = 3 * time.Second
	// defaultFailureRetry bounds how long a store waits after a failed refresh,
	// so a backend that was unreachable at start-up is retried soon rather than
	// after a full interval.
	defaultFailureRetry = 30 * time.Second
	// backgroundRefreshTimeout bounds a refresh started by a stale read, so a
	// hung backend cannot leak the goroutine.
	backgroundRefreshTimeout = 2 * time.Minute
)

// Generation is one immutable snapshot together with the moment it was loaded.
type Generation[T any] struct {
	Value    T
	LoadedAt time.Time
}

// Fetch loads a complete generation. It must not mutate anything it returned
// before: readers may still be serving the previous generation.
type Fetch[T any] func(ctx context.Context) (T, error)

// Options configures a Store or a Named.
type Options struct {
	// Name labels this store in logs and in OnRefresh. It is not unique.
	Name string
	// Interval enables the background refresh loop and sets its period. Zero
	// disables the loop; reads then rely on TTL, if it is set.
	Interval time.Duration
	// TTL is how old a snapshot may be before a read revalidates it in the
	// background. Zero means reads never consider a snapshot stale.
	TTL time.Duration
	// FailureRetry is how long the loop waits after a failed refresh. Zero uses
	// min(Interval, 30s).
	FailureRetry time.Duration
	// Jitter is the fraction by which intervals are randomly adjusted. Zero
	// uses DefaultJitter; a negative value disables it.
	Jitter float64
	// LookupTimeout bounds a Named store's read-through. Zero uses
	// DefaultLookupTimeout.
	LookupTimeout time.Duration
	// OnRefresh, when set, is called after every refresh attempt with its
	// outcome and duration. It runs on the refreshing goroutine and must be
	// cheap.
	OnRefresh func(ctx context.Context, err error, took time.Duration)
}

// withDefaults returns o with every unset field given its default.
func (o Options) withDefaults() Options {
	if o.Interval < 0 {
		o.Interval = 0
	}
	if o.TTL < 0 {
		o.TTL = 0
	}
	if o.FailureRetry <= 0 {
		o.FailureRetry = min(o.Interval, defaultFailureRetry)
		if o.FailureRetry <= 0 {
			o.FailureRetry = defaultFailureRetry
		}
	}
	if o.Jitter == 0 {
		o.Jitter = DefaultJitter
	}
	if o.LookupTimeout <= 0 {
		o.LookupTimeout = DefaultLookupTimeout
	}
	if o.Name == "" {
		o.Name = "astore"
	}
	return o
}

// Store holds one value as an immutable snapshot.
//
// A nil *Store is usable: every method answers "no value" and does nothing,
// which spares callers a nil check on a dependency that a deployment may omit.
// It is safe for concurrent use.
type Store[T any] struct {
	opts  Options
	fetch Fetch[T]

	// ptr publishes the current generation. Readers load it without a lock;
	// writers build the next generation first and store it last.
	ptr atomic.Pointer[Generation[T]]

	// sf collapses concurrent refreshes into one call.
	sf singleflight.Group

	// mu guards lastErr and started. It is never held across a fetch.
	mu      sync.Mutex
	lastErr error
	started bool
}

// New returns a Store that loads its value via fetch.
func New[T any](opts Options, fetch Fetch[T]) *Store[T] {
	return &Store[T]{opts: opts.withDefaults(), fetch: fetch}
}

// Start loads the first snapshot if none is published yet, then refreshes it on
// the configured interval until ctx is done.
//
// It is idempotent: a second call does not start a second loop. The error of
// the first load is returned, but the loop starts regardless — a backend that
// is unreachable at start-up must not require a restart to recover. It is meant
// to be called from the wiring layer, once, with the application's context.
func (s *Store[T]) Start(ctx context.Context) error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.started = true
	s.mu.Unlock()

	var firstErr error
	if _, ok := s.Current(); !ok {
		firstErr = s.Refresh(ctx)
	}
	if s.opts.Interval > 0 {
		go s.run(ctx)
	}
	return firstErr
}

// run refreshes the snapshot on the configured interval until ctx is done.
func (s *Store[T]) run(ctx context.Context) {
	for {
		delay := s.opts.Interval
		if s.err() != nil {
			delay = s.opts.FailureRetry
		}
		timer := time.NewTimer(jitter(delay, s.opts.Jitter))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			_ = s.Refresh(ctx)
		}
	}
}

// Refresh loads a new generation synchronously and publishes it.
//
// Concurrent calls share one load. On failure the previous generation stays
// published and the error is returned, so a caller that needs to know can see
// it; a reader of Load or Current never does.
func (s *Store[T]) Refresh(ctx context.Context) error {
	if s == nil || s.fetch == nil {
		return nil
	}

	_, err, _ := s.sf.Do("refresh", func() (any, error) {
		start := time.Now()
		value, err := s.fetch(ctx)
		took := time.Since(start)

		s.mu.Lock()
		s.lastErr = err
		s.mu.Unlock()

		if err != nil {
			slog.WarnContext(ctx, "astore: refresh failed; keeping the previous snapshot",
				"store", s.opts.Name, "error", err)
			if s.opts.OnRefresh != nil {
				s.opts.OnRefresh(ctx, err, took)
			}
			return nil, err
		}

		s.ptr.Store(&Generation[T]{Value: value, LoadedAt: time.Now()})
		if s.opts.OnRefresh != nil {
			s.opts.OnRefresh(ctx, nil, took)
		}
		slog.DebugContext(ctx, "astore: snapshot refreshed", "store", s.opts.Name, "took", took)
		return nil, nil
	})
	return err
}

// Seed publishes v as the current generation without calling fetch.
//
// It is for a store whose value is known at construction — a static list in a
// test, or a cache built from an already-loaded object — so that the first
// Start does not try to load over it.
func (s *Store[T]) Seed(v T) {
	if s == nil {
		return
	}
	s.ptr.Store(&Generation[T]{Value: v, LoadedAt: time.Now()})
}

// Current returns the published generation without any side effect.
//
// It is the read for code that must not trigger a load: a status endpoint, a
// change detector, or a merge that only applies to data already loaded.
func (s *Store[T]) Current() (Generation[T], bool) {
	if s == nil {
		var zero Generation[T]
		return zero, false
	}
	gen := s.ptr.Load()
	if gen == nil {
		var zero Generation[T]
		return zero, false
	}
	return *gen, true
}

// Load returns the snapshot, refreshing it when caching allows.
//
// A snapshot older than the TTL is returned immediately and refreshed in the
// background. A missing snapshot is loaded synchronously, because a caller that
// has nothing to serve has to wait for something. When both Interval and TTL
// are zero, caching is off and every call loads.
//
// A failed load is not reported here — a caller of Load has nothing better to
// serve either way — but LoadE returns it.
func (s *Store[T]) Load(ctx context.Context) (Generation[T], bool) {
	if s == nil {
		var zero Generation[T]
		return zero, false
	}
	if _, err := s.LoadE(ctx); err != nil {
		if previous, ok := s.Current(); ok {
			return previous, true
		}
		var zero Generation[T]
		return zero, false
	}
	return s.Current()
}

// LoadE is Load with the load error preserved.
//
// An error is reported only when there is nothing to serve: once a generation
// exists, a failing refresh leaves the previous one in place and the error is
// available through Status, which keeps a database blip from turning a cached
// answer into a failed request.
func (s *Store[T]) LoadE(ctx context.Context) (Generation[T], error) {
	if s == nil {
		var zero Generation[T]
		return zero, nil
	}

	gen, ok := s.Current()
	if !ok {
		// Nothing to serve: load now, and a failure is the caller's to handle.
		if err := s.Refresh(ctx); err != nil {
			var zero Generation[T]
			return zero, err
		}
		gen, _ = s.Current()
		return gen, nil
	}

	if s.opts.Interval <= 0 && s.opts.TTL <= 0 {
		// Caching disabled: every read loads, and there is no stale value to
		// fall back on by definition of the mode.
		if err := s.Refresh(ctx); err != nil {
			return gen, err
		}
		gen, _ = s.Current()
		return gen, nil
	}

	if s.opts.TTL > 0 && time.Since(gen.LoadedAt) >= s.opts.TTL {
		s.refreshAsync(ctx)
	}
	return gen, nil
}

// Status reports when the current generation was loaded and why the last
// refresh failed, if it did.
//
// The error is nil after a successful refresh; the timestamp is the zero time
// until something has been loaded.
func (s *Store[T]) Status() (time.Time, error) {
	if s == nil {
		return time.Time{}, nil
	}
	gen, ok := s.Current()
	loadedAt := time.Time{}
	if ok {
		loadedAt = gen.LoadedAt
	}
	return loadedAt, s.err()
}

// Invalidate drops the snapshot, so the next Load or Refresh reads the source
// again. The last error is left as it was: dropping a snapshot says nothing
// about whether the backend is reachable.
func (s *Store[T]) Invalidate() {
	if s == nil {
		return
	}
	s.ptr.Store(nil)
}

// refreshAsync revalidates a stale snapshot without making the reader wait.
func (s *Store[T]) refreshAsync(ctx context.Context) {
	bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), backgroundRefreshTimeout)
	go func() {
		defer cancel()
		defer func() {
			if r := recover(); r != nil {
				slog.Error("astore: panic during background refresh", "store", s.opts.Name, "recover", r)
			}
		}()
		_ = s.Refresh(bg)
	}()
}

// err returns the last refresh error.
func (s *Store[T]) err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

// currentPtr returns the published generation without copying it. The pointer
// is immutable, so it is safe to compare and swap against.
func (s *Store[T]) currentPtr() *Generation[T] {
	return s.ptr.Load()
}

// publish replaces old with next when old is still the published generation,
// preserving its load time: merging data a reader happened to find does not
// make the rest of the snapshot fresher.
func (s *Store[T]) publish(old, next *Generation[T]) bool {
	return s.ptr.CompareAndSwap(old, next)
}

// jitter adjusts d by up to fraction*100 percent, so replicas do not refresh in
// lockstep. A non-positive d or fraction leaves d alone.
func jitter(d time.Duration, fraction float64) time.Duration {
	if d <= 0 || fraction <= 0 {
		return d
	}
	// rand.Float64 returns [0,1); centre it on zero.
	delta := (rand.Float64()*2 - 1) * fraction
	return time.Duration(float64(d) * (1 + delta))
}
