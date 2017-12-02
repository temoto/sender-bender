package junk

import (
	"time"
)

func ResetTimer(t *time.Timer, d time.Duration) {
	t.Reset(d)
	select {
	case <-t.C:
	default:
	}
}
