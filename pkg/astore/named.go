package astore

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"time"

	"golang.org/x/sync/singleflight"
)

// LookupFunc confirms one key against the source when the snapshot does not
// hold it. It reports ErrNotFound (or a configured equivalent) for a row that
// genuinely does not exist.
type LookupFunc[T any] func(ctx context.Context, key string) (*T, error)

// NamedConfig configures a Named store.
type NamedConfig[T any] struct {
	// Options are shared with Store.
	Options
	// Load fetches the full list. It is required.
	Load func(ctx context.Context) ([]*T, error)
	// Key derives the lookup key for an entity. An empty key excludes it from
	// by-key lookup but keeps it in List.
	Key func(*T) string
	// Lookup enables read-through for a key the snapshot does not hold. A nil
	// Lookup, or a non-positive TTL, disables it.
	Lookup LookupFunc[T]
	// NotFound is what GetE returns for a row that does not exist. It defaults
	// to ErrNotFound; a caller with its own sentinel can set it so errors.Is
	// keeps working for its consumers.
	NotFound error
	// IsNotFound recognizes the source's not-found error. It defaults to
	// errors.Is against NotFound and ErrNotFound.
	IsNotFound func(error) bool
}

// Named is a stale-while-revalidate cache over a keyed list of entities.
//
// The list is loaded as one immutable snapshot, which serves O(1) keyed lookups
// and full listings. A lookup for a key the snapshot does not hold can be
// confirmed against the source (see NamedConfig.Lookup): a snapshot describes
// one instant, and a row written after that instant is absent from it until the
// next refresh.
//
// A nil *Named is usable and answers "not found", which spares call sites a nil
// check for a dependency a deployment may omit.
type Named[T any] struct {
	store         *Store[*snapshot[T]]
	ttl           time.Duration
	key           func(*T) string
	lookup        LookupFunc[T]
	lookupTimeout time.Duration
	notFound      error
	isNotFound    func(error) bool
	sf            singleflight.Group
}

// snapshot is an immutable keyed view of a resource list.
type snapshot[T any] struct {
	byKey map[string]*T
	all   []*T
}

// absent marks "the source says this row does not exist" inside singleflight,
// where a nil value cannot mean success.
type absent struct{}

// NewNamed builds a Named store.
func NewNamed[T any](cfg NamedConfig[T]) *Named[T] {
	opts := cfg.Options.withDefaults()

	notFound := cfg.NotFound
	if notFound == nil {
		notFound = ErrNotFound
	}
	isNotFound := cfg.IsNotFound
	if isNotFound == nil {
		isNotFound = func(err error) bool {
			return errors.Is(err, notFound) || errors.Is(err, ErrNotFound)
		}
	}

	key := cfg.Key
	if key == nil {
		key = func(*T) string { return "" }
	}

	build := func(ctx context.Context) (*snapshot[T], error) {
		items, err := cfg.Load(ctx)
		if err != nil {
			return nil, err
		}
		byKey := make(map[string]*T, len(items))
		for _, item := range items {
			if item == nil {
				continue
			}
			if k := key(item); k != "" {
				byKey[k] = item
			}
		}
		return &snapshot[T]{byKey: byKey, all: items}, nil
	}

	return &Named[T]{
		store:         New(opts, build),
		ttl:           opts.TTL,
		key:           key,
		lookup:        cfg.Lookup,
		lookupTimeout: opts.LookupTimeout,
		notFound:      notFound,
		isNotFound:    isNotFound,
	}
}

// Start loads the first snapshot if none is published yet, then refreshes it on
// the configured interval. It is idempotent.
func (n *Named[T]) Start(ctx context.Context) error {
	if n == nil {
		return nil
	}
	return n.store.Start(ctx)
}

// Refresh reloads the full list synchronously.
func (n *Named[T]) Refresh(ctx context.Context) error {
	if n == nil {
		return nil
	}
	return n.store.Refresh(ctx)
}

// Status reports when the snapshot was loaded and why the last refresh failed.
func (n *Named[T]) Status() (time.Time, error) {
	if n == nil {
		return time.Time{}, nil
	}
	return n.store.Status()
}

// Get returns the entity for name and whether it exists.
func (n *Named[T]) Get(ctx context.Context, name string) (*T, bool) {
	item, err := n.GetE(ctx, name)
	if err != nil || item == nil {
		return nil, false
	}
	return item, true
}

// GetE is Get with the failure preserved: a row that does not exist reports the
// configured NotFound sentinel, and a read that failed reports its error.
func (n *Named[T]) GetE(ctx context.Context, name string) (*T, error) {
	if n == nil {
		return nil, ErrNotFound
	}

	gen, loadErr := n.store.LoadE(ctx)
	if loadErr == nil && gen.Value != nil {
		if item, ok := gen.Value.byKey[name]; ok {
			return item, nil
		}
	}

	if n.lookup == nil || n.ttl <= 0 || name == "" {
		if loadErr != nil {
			return nil, loadErr
		}
		return nil, n.notFound
	}

	// A point read can still succeed when the full-list load failed; trying it
	// keeps a transient list error from failing a by-key read that would work.
	return n.lookupMissing(ctx, name)
}

// List returns all cached entities. The returned slice is shared and must not
// be modified; ListE returns the same slice with the load error preserved.
func (n *Named[T]) List(ctx context.Context) []*T {
	items, _ := n.ListE(ctx)
	return items
}

// ListE returns every cached entity, or the error that prevented a snapshot
// from being loaded at all.
func (n *Named[T]) ListE(ctx context.Context) ([]*T, error) {
	if n == nil {
		return nil, nil
	}
	gen, err := n.store.LoadE(ctx)
	if err != nil {
		slog.WarnContext(ctx, "astore: snapshot load failed", "store", n.store.opts.Name, "error", err)
		return nil, err
	}
	if gen.Value == nil {
		return nil, nil
	}
	return gen.Value.all, nil
}

// Merge adds or replaces one entity without making the rest of the snapshot
// look fresher than it is.
//
// It is what read-through uses. The merge is a copy-on-write publish guarded by
// a compare-and-swap, so a concurrent full refresh is never overwritten with
// data derived from the generation it replaced; Merge may therefore retry, and
// n.key must stay pure.
func (n *Named[T]) Merge(item *T) {
	if n == nil || item == nil {
		return
	}
	for {
		old := n.store.currentPtr()
		if old == nil || old.Value == nil {
			// A cache that has never loaded has nothing to merge into: filling
			// it from one key would turn the next List into a one-item answer.
			return
		}
		next := old.Value.with(item, n.key)
		if next == old.Value {
			return
		}
		if n.store.publish(old, &Generation[*snapshot[T]]{Value: next, LoadedAt: old.LoadedAt}) {
			return
		}
	}
}

// with returns a copy of s that also holds item.
//
// Copy-on-write is not an optimisation here but the correctness argument: Get
// and List read a snapshot without a lock, so writing into the shared map or
// appending to the shared slice would race with every reader alive at that
// moment. A reader holding the old pointer keeps seeing the old view, which is
// the view it asked for.
//
// An item already present in the list is replaced rather than appended: a full
// refresh may have brought the same row in between the miss and the lookup, and
// List must not show it twice.
func (s *snapshot[T]) with(item *T, key func(*T) string) *snapshot[T] {
	k := key(item)
	if k == "" {
		return s
	}

	byKey := make(map[string]*T, len(s.byKey)+1)
	maps.Copy(byKey, s.byKey)
	byKey[k] = item

	all := make([]*T, len(s.all), len(s.all)+1)
	copy(all, s.all)
	for i, it := range all {
		if it != nil && key(it) == k {
			all[i] = item
			return &snapshot[T]{byKey: byKey, all: all}
		}
	}
	return &snapshot[T]{byKey: byKey, all: append(all, item)}
}

// lookupMissing asks the source for one row and merges what it finds.
func (n *Named[T]) lookupMissing(ctx context.Context, name string) (*T, error) {
	// One query per key at a time. Polling for a row that does not exist yet is
	// a normal shape here (a caller retrying until a peer's write lands), and
	// every one of those requests would otherwise ask the same question.
	//
	// The query is detached from this caller's cancellation but still bounded:
	// its answer is shared with waiters that did not cancel, and one abandoned
	// request must not turn their hit into a miss.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), n.lookupTimeout)
	defer cancel()

	v, err, _ := n.sf.Do(name, func() (any, error) {
		item, err := n.lookup(ctx, name)
		if err != nil {
			if n.isNotFound(err) {
				return absent{}, nil
			}
			return nil, err
		}
		if item == nil {
			return absent{}, nil
		}
		// Best effort: a concurrent full refresh may replace this snapshot.
		// Correctness does not depend on the merge surviving — the next miss
		// reads the source again — it only saves the following reader a query.
		n.Merge(item)
		return item, nil
	})
	if err != nil {
		slog.WarnContext(ctx, "astore: read-through failed", "store", n.store.opts.Name, "key", name, "error", err)
		return nil, err
	}
	if _, ok := v.(absent); ok {
		return nil, n.notFound
	}
	item, _ := v.(*T)
	if item == nil {
		return nil, n.notFound
	}
	return item, nil
}
