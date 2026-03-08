package task

import (
	"context"
	"time"
)

type mergedContext struct {
	a    context.Context
	b    context.Context
	done chan struct{}
	err  error
}

func (c *mergedContext) Done() <-chan struct{} {
	return c.done
}

func (c *mergedContext) Err() error {
	return c.err
}

func (c *mergedContext) Deadline() (time.Time, bool) {
	d1, ok1 := c.b.Deadline()
	d2, ok2 := c.a.Deadline()
	if ok1 && d1.UnixNano() < d2.UnixNano() {
		return d1, true
	}
	if ok2 {
		return d2, true
	}
	return time.Time{}, false
}

func (c *mergedContext) Value(key any) any {
	b := c.b.Value(key)
	if b != nil {
		return b
	}
	return c.a.Value(key)
}

func mergeContexts(a, b context.Context) context.Context {
	c := &mergedContext{
		a:    a,
		b:    b,
		done: make(chan struct{}),
	}
	go func() {
		var err error
		select {
		case <-a.Done():
			err = a.Err()

		case <-b.Done():
			err = b.Err()
		}

		c.err = err
		close(c.done)
	}()
	return c
}
