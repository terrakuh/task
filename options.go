package task

import (
	"context"
	"math"
	"time"
)

type (
	Option func(*runCtx)

	RetryFunc func(h *RawHandle, err error) (time.Time, error)

	OnAfterCallFunc[I, O any] func(h *Handle[I, O])
)

// WithAfterRun this task will run after the completion of id, regardless of success or error.
func WithAfterRun[I, O any](h *Handle[I, O]) Option {
	return func(rc *runCtx) {
		rc.dependencies = append(rc.dependencies, dependency{
			done:          h.Done,
			genericHandle: &GenericHandle{&h.RawHandle, h},
			predicate:     func() bool { return true },
		})
	}
}

// WithAfterExternal adds a dependency that will resolve once done is closed or returns a value.
func WithAfterExternal(done chan struct{}) Option {
	return func(rc *runCtx) {
		rc.dependencies = append(rc.dependencies, dependency{
			done:          done,
			genericHandle: nil,
			predicate:     func() bool { return true },
		})
	}
}

// WithExpRetries retries the function with 2**x interval (from 2**-3 up to 2**15 or n). Meaning:
//
//	125ms, 250ms, 500ms, 1s, 2s, 4s, ..., 9h6m8s
func WithExpRetries(n int64) Option {
	end := min(-3+n, 15)
	return WithExp2Retries(-3, end, n-(end+3))
}

func WithExp2Retries(start, end, extra int64) Option {
	if start > end || extra < 0 {
		panic("invalid args")
	}
	return WithRetryFunc(func(h *RawHandle, err error) (time.Time, error) {
		i := min(h.Retried+start, end)
		if i == end && h.Retried+start-end >= extra {
			return time.Time{}, err
		}

		d := time.Duration(math.Exp2(float64(i))*1_000) * time.Millisecond
		return time.Now().Add(d), nil
	})
}

func WithRetryFunc(retry RetryFunc) Option {
	return func(rc *runCtx) {
		rc.retry = retry
	}
}

// WithRunAt will schedule the task to run after the given time has passed.
func WithRunAt(time time.Time) Option {
	return func(rc *runCtx) {
		rc.runAt = time
	}
}

// WithContext will add an additional context to the task. This context will be merged
// with the initial context from [New].
func WithContext(ctx context.Context) Option {
	return func(rc *runCtx) {
		rc.ctx = ctx
	}
}

// WithOnAfterCall registers a callback that is called each time after the task has run.
// This callback is executed before the retry function.
func WithOnAfterCall[I, O any](fn OnAfterCallFunc[I, O]) Option {
	return func(rc *runCtx) {
		rc.onAfterCall = fn
	}
}
