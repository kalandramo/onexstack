package astore

import (
	"context"
	"log/slog"
	"time"
)

// Run calls fn once synchronously, then on the configured interval until ctx is
// done, and returns the error of that first call.
//
// It exists for periodic work that is more than loading one snapshot — a rule
// reload that also republishes a compiled policy, say — so that work shares the
// loop semantics of a Store: the first call is synchronous, a failed first call
// still starts the loop, and a failed pass is retried after FailureRetry rather
// than after a full interval.
//
// The loop runs on its own goroutine once the first call returns. With a
// non-positive Interval there is no loop and Run is a single call.
func Run(ctx context.Context, opts Options, fn func(context.Context) error) error {
	opts = opts.withDefaults()

	firstErr := callOnce(ctx, opts, fn)
	if opts.Interval <= 0 {
		return firstErr
	}

	go func() {
		lastErr := firstErr
		for {
			delay := opts.Interval
			if lastErr != nil {
				delay = opts.FailureRetry
			}
			timer := time.NewTimer(jitter(delay, opts.Jitter))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				lastErr = callOnce(ctx, opts, fn)
			}
		}
	}()

	return firstErr
}

// callOnce runs one pass and reports its outcome.
func callOnce(ctx context.Context, opts Options, fn func(context.Context) error) error {
	start := time.Now()
	err := fn(ctx)
	took := time.Since(start)
	if opts.OnRefresh != nil {
		opts.OnRefresh(ctx, err, took)
	}
	if err == nil {
		slog.DebugContext(ctx, "astore: periodic refresh completed", "store", opts.Name, "took", took)
	}
	if err != nil {
		slog.WarnContext(ctx, "astore: periodic refresh failed; will retry",
			"store", opts.Name, "error", err)
	}
	return err
}
