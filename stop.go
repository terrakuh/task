package task

type Stop int

const (
	// StopGracefully will cause the stop function to wait until all active and pending tasks are completed.
	StopGracefully Stop = iota
	// StopImmediately will signal all active tasks to stop via [context.Context]. Pending tasks will not be run.
	StopImmediately
)

func (m *manager[I, O]) Stop(stop Stop) {
	close(m.stopped)

	switch stop {
	case StopGracefully:

	case StopImmediately:
		m.cancel(ErrStopped)
	}

	m.running.Wait()
}
