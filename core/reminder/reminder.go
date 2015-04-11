// Package reminder provides a mechanism for receiving reminders after certain events have happened.
package reminder

import (
	"github.com/runningwild/clock"
	"sync"
	"time"
)

// MakeBatchReminder creates and returns a Batch reminder object.  Batch.Close() should be called
// when it is no longer needed.
func MakeBatchReminder() *Batch {
	return MakeBatchReminderWithClock(&clock.RealClock{})
}

// MakeBatchReminder creates and returns a Batch reminder object using the specified clock.
// Batch.Close() should be called when it is no longer needed.
func MakeBatchReminderWithClock(c clock.Clock) *Batch {
	b := &Batch{
		c:        c,
		signal:   make(chan int),
		postpone: make(chan time.Time),
	}
	go b.run()
	return b
}

// Batch provides a mechanism for receiving a reminder after one or more events have occurred.
// Batch.Ping(delay) will cause a value to be sent on Batch.Wait() after delay has elapsed OR
// Batch.Ping() has been called again.  The value sent on Batch.Wait() will be the number of times
// Ping() was called before the value was sent.
type Batch struct {
	c clock.Clock

	signal chan int

	m        sync.Mutex
	postpone chan time.Time
}

func (b *Batch) run() {
	var at <-chan time.Time
	count := 0
	for {
		select {
		case <-at:
			b.signal <- count
			count = 0
		case t, ok := <-b.postpone:
			if !ok {
				return
			}
			at = b.c.At(t)
			count++
		}
	}
}

// Ping will cause a value to be sent along Wait() after wait has elapsed, unless Ping is called
// again before then.
func (b *Batch) Ping(wait time.Duration) {
	b.postpone <- b.c.Now().Add(wait)
}

// Wait blocks until Ping(wait) is called and wait elapses without Ping() being called again.  The
// value received from Wait() will be a count of how many times Ping() was called since the last
// value was received from Wait().
func (b *Batch) Wait() <-chan int {
	return b.signal
}

// Close shuts down the Batch and cleans up the associated goroutine.
func (b *Batch) Close() {
	close(b.postpone)
}
