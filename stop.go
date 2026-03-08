package task

type Stop int

const (
	StopGracefully Stop = iota
	StopImmediately
)

// Stop will cause the manager to reject any new tasks and start finishing up.
func (m *manager[I, O]) Stop(stop Stop) {
	close(m.stopped)

	switch stop {
	case StopGracefully:

	case StopImmediately:
		m.cancel(ErrStopped)
	}

	m.running.Wait()
}
