// Package astore keeps read-mostly data in memory as an immutable snapshot.
//
// Reads never touch the backend: they return the snapshot published by the last
// successful load, so a request path that consults a setting, a role list or a
// resource table pays a memory read and nothing else.
//
// A snapshot is replaced as a whole. Loaders build the next generation off to
// the side and publish it with a single atomic store, which means a reader sees
// either the old generation or the new one and never a half-applied mixture. A
// failed load keeps the previous generation in place: stale data is preferred
// to no data, and the failure is reported through Status, the logs and the
// OnRefresh hook.
//
// Two refresh styles compose:
//
//   - Interval > 0 refreshes the snapshot on a timer, whether or not anyone
//     reads it. This is what keeps an idle service from serving a snapshot that
//     went stale while no request was arriving.
//   - TTL > 0 revalidates lazily: a read that finds the snapshot older than the
//     TTL returns the snapshot immediately and refreshes it in the background.
//
// Setting both is the intended default: the timer does the ordinary work, and
// the TTL is the safety net for a timer that has not fired yet. Setting neither
// disables caching, and every read loads.
//
// Store[T] holds one value. Named[T] holds a keyed list and can confirm a miss
// against the source, which is what makes a create followed by a read work
// across replicas without waiting for the next refresh.
package astore
