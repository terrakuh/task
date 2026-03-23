package task

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpRetry(t *testing.T) {
	rc := runCtx{}
	expectedErr := errors.New("")

	WithExpRetries(2)(&rc)
	for i := range 3 {
		h := RawHandle{Retried: int64(i)}
		next, err := rc.retry(&h, expectedErr)
		if i == 2 {
			require.ErrorIs(t, err, expectedErr)
		} else {
			require.NoError(t, err)
			assert.InDelta(t, time.Until(next), (125<<int64(i))*time.Millisecond, float64(20*time.Millisecond), i)
		}
	}

	WithExpRetries(20)(&rc)
	for i := range 21 {
		h := RawHandle{Retried: int64(i)}
		next, err := rc.retry(&h, expectedErr)
		if i == 20 {
			require.ErrorIs(t, err, expectedErr)
		} else {
			require.NoError(t, err)
			assert.InDelta(t, time.Until(next),
				min((125<<int64(i))*time.Millisecond, 9*time.Hour+6*time.Minute+8*time.Second), float64(20*time.Millisecond), i)
		}
	}
}
