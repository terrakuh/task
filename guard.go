package task

import "sync"

type guard struct {
	l      sync.Locker
	locked bool
}

func lock(l sync.Locker) guard {
	l.Lock()
	return guard{l, true}
}

func (g *guard) unlock() {
	if g.locked {
		g.l.Unlock()
		g.locked = false
	}
}
