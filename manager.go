package task

import (
	"context"
	"errors"
	"sync"
	"time"
)

type (
	ID uint64

	Manager[I, O any] interface {
		// Do will post the given task to one of the active workers. If the backlog is full, Do will block
		// until the task was posted, unless any associated context was cancelled or the task manager was stopped.
		Do(f Task[I, O], input I, options ...Option) (*Handle[I, O], error)

		// Stop will cause the manager to reject any new tasks and start finishing up.
		// Once all tasks are finished and the workers have joined the function will return.
		//
		// Calling Stop multiple times is not allowed.
		Stop(stop Stop)
	}

	Context[I, O any] struct {
		Dependencies []*Handle[I, O]
	}

	RawHandle struct {
		// Done is closed once the task has finished and no further retries are pending.
		Done chan struct{}
		// ID is a - for a task manager instance - unique ID for each task. This has no further meaning.
		ID ID
		// Err is set to the final resulting error of the task once [RawHandle.Done] is closed.
		Err error
		// Panic will store the recovered panic from the task. The task will not be retried after a panic.
		Panic  any
		Cancel context.CancelFunc
		// Retried is the amount of retried runs of the task.
		Retried int64
	}

	Handle[I, O any] struct {
		RawHandle

		// Input is input argument for the task.
		Input  I
		Output O
	}

	Task[I, O any] func(ctx context.Context, in I) (out O, err error)

	manager[I, O any] struct {
		ctx     context.Context
		cancel  context.CancelCauseFunc
		stopped chan struct{}
		// running is the currently active Goroutines.
		running sync.WaitGroup

		mu        sync.Mutex
		idCounter ID
		backlog   chan *task[I, O]
	}

	task[I, O any] struct {
		runCtx

		handle *Handle[I, O]
		task   Task[I, O]
	}

	runCtx struct {
		ctx          context.Context
		retry        RetryFunc
		runAt        time.Time
		dependencies []dependency
	}

	dependency struct {
		done      chan struct{}
		predicate func() bool
	}

	contextKey struct{}
)

var (
	_ (Manager[any, any]) = (*manager[any, any])(nil)

	ErrStopped = errors.New("manager stopped")
)

// For will return the context info inside a task. Calling this function outside is not allowed.
func For[I, O any](ctx context.Context) Context[I, O] {
	val, ok := ctx.Value(contextKey{}).(Context[I, O])
	if !ok {
		panic("task.For must be used inside of a task")
	}
	return val
}

// New creates a new task manager with n active worker goroutines and a task backlog of backlog.
// Once all workers are busy and the backlog is full, any further call to [Manager.Do] will block.
// The given context will be used as the root for the workers and any tasks.
func New[I, O any](ctx context.Context, n, backlog int) Manager[I, O] {
	if n <= 0 {
		panic("invalid worker count")
	}
	if backlog < 0 {
		panic("invalid backlog size")
	}

	ctx, cancel := context.WithCancelCause(ctx)
	m := &manager[I, O]{
		ctx:     ctx,
		cancel:  cancel,
		stopped: make(chan struct{}),
		running: sync.WaitGroup{},

		mu:        sync.Mutex{},
		idCounter: 1,
		backlog:   make(chan *task[I, O], backlog),
	}

	// Launch workers.
	for range n {
		m.running.Go(m.worker)
	}

	return m
}

func (m *manager[I, O]) Do(f Task[I, O], input I, options ...Option) (*Handle[I, O], error) {
	g := lock(&m.mu)
	defer g.unlock()

	select {
	case <-m.stopped:
		return nil, ErrStopped

	default:
	}

	id := m.idCounter
	m.idCounter++
	h := &Handle[I, O]{
		RawHandle: RawHandle{
			Done:  make(chan struct{}),
			ID:    id,
			Err:   nil,
			Panic: nil,
		},
		Input: input,
	}

	task := &task[I, O]{
		runCtx: runCtx{
			ctx:   nil,
			retry: nil,
		},
		handle: h,
		task:   f,
	}
	for _, option := range options {
		option(&task.runCtx)
	}

	if task.ctx == nil {
		task.ctx = m.ctx
	} else {
		task.ctx = mergeContexts(m.ctx, task.ctx)
	}

	task.ctx, h.Cancel = context.WithCancel(task.ctx)

	// This task has a complex dependency structure and must be awaited.
	if len(task.dependencies) != 0 || time.Now().Before(task.runAt) {
		go m.waitDependencies(task)
		return h, nil
	}

	// Can be executed immediately.
	g.unlock()
	m.queueTask(task, true)

	return h, nil
}

func (m *manager[I, O]) worker() {
	for {
		select {
		case <-m.ctx.Done():
			return

		case task, ok := <-m.backlog:
			if !ok {
				return
			}
			m.doTask(task)
		}
	}
}

func (m *manager[I, O]) queueTask(task *task[I, O], block bool) {
	if block {
		select {
		case <-task.ctx.Done():
		case <-m.stopped:
		case m.backlog <- task:
		}
	} else {
		select {
		case <-task.ctx.Done():
		case <-m.stopped:
		case m.backlog <- task:

		default:
			go m.queueTask(task, true)
		}
	}
}

func (m *manager[I, O]) waitDependencies(task *task[I, O]) {
	for _, dep := range task.dependencies {
		select {
		case <-task.ctx.Done():
			return

		case <-dep.done:
			if !dep.predicate() {
				return
			}
		}
	}

	sleep := time.Since(task.runAt)
	if sleep < 0 {
		select {
		case <-task.ctx.Done():
			return

		case <-time.After(-sleep):
		}
	}

	m.queueTask(task, true)
}

func (m *manager[I, O]) doTask(task *task[I, O]) {
	defer func() {
		if task.handle.Panic = recover(); task.handle.Panic != nil {
			// Panics will not retried.
			task.handle.Cancel()
			close(task.handle.Done)
		}
	}()

	output, err := task.task(task.ctx, task.handle.Input)

	// Retry.
	if err != nil && task.retry != nil && !errors.Is(err, context.Canceled) {
		after, err := task.retry(&task.handle.RawHandle, err)
		// Put back into queue.
		if err == nil {
			task.handle.Retried++
			task.runAt = after
			if after.Before(time.Now()) {
				m.queueTask(task, false)
			} else {
				go m.waitDependencies(task)
			}
			return
		}
	}

	// Task finished.
	if err != nil {
		task.handle.Err = err
	} else {
		task.handle.Output = output
	}

	task.handle.Cancel()
	close(task.handle.Done)
}
