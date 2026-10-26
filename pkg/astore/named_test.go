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

type widget struct {
	Name string
	Ver  int
}

// widgetDB is a test source for a keyed list.
type widgetDB struct {
	mu        sync.Mutex
	rows      []*widget
	listCalls atomic.Int64
	getCalls  atomic.Int64
	listErr   error
	getErr    error
	getDelay  time.Duration
}

func (d *widgetDB) list(context.Context) ([]*widget, error) {
	d.listCalls.Add(1)
	d.mu.Lock()
	rows, err := append([]*widget(nil), d.rows...), d.listErr
	d.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (d *widgetDB) get(ctx context.Context, name string) (*widget, error) {
	d.getCalls.Add(1)
	if d.getDelay > 0 {
		select {
		case <-time.After(d.getDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.getErr != nil {
		return nil, d.getErr
	}
	for _, row := range d.rows {
		if row.Name == name {
			clone := *row
			return &clone, nil
		}
	}
	return nil, ErrNotFound
}

func (d *widgetDB) set(rows ...*widget) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rows = rows
}

func newWidgets(d *widgetDB, opts Options) *Named[widget] {
	return NewNamed(NamedConfig[widget]{
		Options: opts,
		Load:    d.list,
		Key:     func(w *widget) string { return w.Name },
		Lookup:  d.get,
	})
}

func TestNamed_HitNeedsNoLookup(t *testing.T) {
	ctx := context.Background()
	db := &widgetDB{}
	db.set(&widget{Name: "a", Ver: 1})
	n := newWidgets(db, Options{Name: "t", TTL: time.Minute})
	require.NoError(t, n.Refresh(ctx))

	w, ok := n.Get(ctx, "a")
	require.True(t, ok)
	assert.Equal(t, 1, w.Ver)
	assert.Equal(t, int64(0), db.getCalls.Load())
}

func TestNamed_MissReadsThrough(t *testing.T) {
	ctx := context.Background()
	db := &widgetDB{}
	db.set(&widget{Name: "a", Ver: 1})
	n := newWidgets(db, Options{Name: "t", TTL: time.Minute})
	require.NoError(t, n.Refresh(ctx))

	// Written by another replica: absent from the snapshot, present in the source.
	db.set(&widget{Name: "a", Ver: 1}, &widget{Name: "b", Ver: 2})
	w, ok := n.Get(ctx, "b")
	require.True(t, ok)
	assert.Equal(t, 2, w.Ver)
	assert.Equal(t, int64(1), db.getCalls.Load())

	// The merge saves the next reader a query and keeps one entry per key.
	before, _ := n.Status()
	w, ok = n.Get(ctx, "b")
	require.True(t, ok)
	assert.Equal(t, 2, w.Ver)
	assert.Equal(t, int64(1), db.getCalls.Load())
	after, _ := n.Status()
	assert.Equal(t, before, after, "a read-through must not make the snapshot look fresher")

	names := map[string]int{}
	for _, row := range n.List(ctx) {
		names[row.Name]++
	}
	assert.Equal(t, map[string]int{"a": 1, "b": 1}, names, "List must not show the merged row twice")
}

func TestNamed_MissingRowIsNotCached(t *testing.T) {
	ctx := context.Background()
	db := &widgetDB{}
	n := newWidgets(db, Options{Name: "t", TTL: time.Minute})
	require.NoError(t, n.Refresh(ctx))

	_, ok := n.Get(ctx, "ghost")
	assert.False(t, ok)
	_, ok = n.Get(ctx, "ghost")
	assert.False(t, ok)
	assert.Equal(t, int64(2), db.getCalls.Load(), "a missing row must be asked about again")
}

func TestNamed_GetReportsTheConfiguredSentinel(t *testing.T) {
	ctx := context.Background()
	db := &widgetDB{}
	n := NewNamed(NamedConfig[widget]{
		Options:  Options{Name: "t", TTL: time.Minute},
		Load:     db.list,
		Key:      func(w *widget) string { return w.Name },
		Lookup:   db.get,
		NotFound: errTestNotFound,
	})
	require.NoError(t, n.Refresh(ctx))

	_, err := n.GetE(ctx, "ghost")
	assert.ErrorIs(t, err, errTestNotFound)
}

var errTestNotFound = errors.New("test: not found")

func TestNamed_ConcurrentMissesShareOneLookup(t *testing.T) {
	ctx := context.Background()
	db := &widgetDB{getDelay: 100 * time.Millisecond}
	db.set(&widget{Name: "a", Ver: 1})
	n := newWidgets(db, Options{Name: "t", TTL: time.Minute})
	require.NoError(t, n.Refresh(ctx))

	db.set(&widget{Name: "a", Ver: 1}, &widget{Name: "b", Ver: 2})

	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(12)
	for i := 0; i < 12; i++ {
		go func() {
			defer wg.Done()
			<-start
			_, _ = n.Get(ctx, "b")
		}()
	}
	close(start)
	wg.Wait()

	assert.Equal(t, int64(1), db.getCalls.Load())
}

func TestNamed_ReadThroughWhenTheListLoadFails(t *testing.T) {
	ctx := context.Background()
	db := &widgetDB{listErr: errors.New("list down")}
	db.set(&widget{Name: "a", Ver: 1})
	n := newWidgets(db, Options{Name: "t", TTL: time.Minute})

	w, ok := n.Get(ctx, "a")
	require.True(t, ok, "a point read can succeed while the list load fails")
	assert.Equal(t, 1, w.Ver)
}

func TestNamed_ReadThroughIsOffWithoutTTL(t *testing.T) {
	ctx := context.Background()
	db := &widgetDB{}
	db.set(&widget{Name: "a", Ver: 1})
	n := newWidgets(db, Options{Name: "t", Jitter: -1})

	n.Get(ctx, "a")
	n.Get(ctx, "a")
	assert.Equal(t, int64(0), db.getCalls.Load())
	assert.Equal(t, int64(2), db.listCalls.Load(), "caching is off, so every read lists")
}

func TestNamed_NilIsUsable(t *testing.T) {
	var n *Named[widget]

	_, ok := n.Get(context.Background(), "x")
	assert.False(t, ok)
	assert.Nil(t, n.List(context.Background()))
	n.Merge(&widget{Name: "x"})
	assert.NoError(t, n.Start(context.Background()))
	assert.NoError(t, n.Refresh(context.Background()))
}
