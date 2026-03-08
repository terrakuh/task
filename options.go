package task

import (
	"context"
	"math"
	"time"
)

type (
	Option func(*runCtx)

	RetryFunc func(h *RawHandle, err error) (time.Time, error)
)

// WithAfterRun this task will run after the completion of id, regardless of success or error.
func WithAfterRun[I, O any](h *Handle[I, O]) Option {
	return func(rc *runCtx) {
		rc.dependencies = append(rc.dependencies, dependency{
			done:      h.Done,
			predicate: func() bool { return true },
		})
	}
}

// WithExpRetries retries the function with 2**x interval (from 2**-3 up to 2**25 or n). Meaning:
//
//	125ms, 250ms, 500ms, 1s, 2s, 4s, ..., 9h19m14s432ms
func WithExpRetries(n int64) Option {
	return WithExp2Retries(-3, 25, n)
}

func WithExp2Retries(start, end, n int64) Option {
	return WithRetryFunc(func(h *RawHandle, err error) (time.Time, error) {
		if h.Retried >= n {
			return time.Time{}, err
		}

		i := min(h.Retried, end) + start
		d := time.Duration(math.Exp2(float64(i))*1_000) * time.Millisecond
		return time.Now().Add(d), nil
	})
}

func WithRetryFunc(retry RetryFunc) Option {
	return func(rc *runCtx) {
		rc.retry = retry
	}
}

func WithRunAt(time time.Time) Option {
	return func(rc *runCtx) {
		rc.runAt = time
	}
}

func WithContext(ctx context.Context) Option {
	return func(rc *runCtx) {
		rc.ctx = ctx
	}
}
