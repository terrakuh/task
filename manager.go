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

	Context struct {
		Dependencies []GenericHandle
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
		OK     bool
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

	GenericHandle struct {
		Raw  *RawHandle
		Full any
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
		onAfterCall  any
		runAt        time.Time
		dependencies []dependency
	}

	dependency struct {
		done          chan struct{}
		genericHandle *GenericHandle
		predicate     func() bool
	}

	contextKey struct{}
)

var (
	_ (Manager[any, any]) = (*manager[any, any])(nil)

	ErrStopped = errors.New("manager stopped")
)

// For will return the context info inside a task. Calling this function outside is not allowed.
func For(ctx context.Context) Context {
	val, ok := ctx.Value(contextKey{}).(Context)
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
	task := createTask(m.ctx, f, input, options...)

	g := lock(&m.mu)
	defer g.unlock()

	select {
	case <-m.stopped:
		return nil, ErrStopped

	default:
	}

	task.handle.ID = m.idCounter
	m.idCounter++

	// This task has a complex dependency structure and must be awaited.
	if len(task.dependencies) != 0 || time.Now().Before(task.runAt) {
		go m.waitDependencies(task)
		return task.handle, nil
	}

	// Can be executed immediately.
	g.unlock()
	m.queueTask(task, true)

	return task.handle, nil
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

	task.handle.Err = err
	task.handle.OK = err == nil
	if err == nil {
		task.handle.Output = output
	}

	if task.onAfterCall != nil {
		task.onAfterCall.(OnAfterCallFunc[I, O])(task.handle)
	}

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
	task.handle.Cancel()
	close(task.handle.Done)
}

func createTask[I, O any](ctx context.Context, f Task[I, O], input I, options ...Option) *task[I, O] {
	task := &task[I, O]{
		runCtx: runCtx{
			ctx:   nil,
			retry: nil,
		},
	}

	for _, option := range options {
		option(&task.runCtx)
	}

	// Check types of relevant options.
	if task.onAfterCall != nil {
		_, ok := task.onAfterCall.(OnAfterCallFunc[I, O])
		if !ok {
			panic("invalid OnAfterCallFunc")
		}
	}

	taskCtx := Context{}
	for _, dep := range task.dependencies {
		if dep.genericHandle != nil {
			taskCtx.Dependencies = append(taskCtx.Dependencies, *dep.genericHandle)
		}
	}

	task.handle = &Handle[I, O]{
		RawHandle: RawHandle{
			Done:  make(chan struct{}),
			Err:   nil,
			Panic: nil,
		},
		Input: input,
	}
	task.task = f

	if task.ctx == nil {
		task.ctx = ctx
	} else {
		task.ctx = mergeContexts(ctx, task.ctx)
	}

	task.ctx, task.handle.Cancel = context.WithCancel(task.ctx)
	task.ctx = context.WithValue(task.ctx, contextKey{}, taskCtx)

	return task
}
