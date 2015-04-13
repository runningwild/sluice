package core

import (
	"github.com/runningwild/clock"
	"time"
)

// MakeStreamReminder creates and returns a Stream reminder object.  Stream.Close() should be called
// when it is no longer needed.
func MakeStreamReminder(min, max time.Duration, c clock.Clock) *StreamReminder {
	b := &StreamReminder{
		c:       c,
		updates: make(chan updateItem),
		signal:  make(chan []StreamId),
		min:     min,
		max:     max,
	}
	go b.run()
	return b
}

// StreamReminder provides a mechanism for tracking streams that need work done on them
// periodically.  If Update(stream) is called then Wait() will return stream at some point in the
// future unless Update(stream) is called regularly or Clear(stream) is called.
type StreamReminder struct {
	c clock.Clock

	updates  chan updateItem
	signal   chan []StreamId
	min, max time.Duration
}

type updateItem struct {
	stream StreamId
	t      time.Time
}

func (b *StreamReminder) run() {
	defer close(b.signal)
	var trigger <-chan time.Time

	var signal chan<- []StreamId
	var signalVal []StreamId

	// Map from StreamId to the last time that stream was updated.
	active := make(map[StreamId]time.Time)
	for {
		select {
		case now := <-trigger:
			streams := make(map[StreamId]bool)
			var minRemaining time.Time
			for stream, t := range active {
				if now.Sub(t).Nanoseconds() > b.min.Nanoseconds() {
					streams[stream] = true
				}
			}
			for stream := range streams {
				active[stream] = now
			}
			for _, t := range active {
				if minRemaining.IsZero() || t.Nanosecond() < minRemaining.Nanosecond() {
					minRemaining = t
				}
			}
			if !minRemaining.IsZero() {
				trigger = b.c.At(minRemaining.Add(b.max))
			}

			// Add all previously signaled streams to the new signal, unless those streams are no
			// longer active.
			for _, previousStream := range signalVal {
				if _, ok := active[previousStream]; ok {
					streams[previousStream] = true
				} else {
				}
			}
			if len(streams) > 0 {
				signalVal = signalVal[0:0]
				for stream := range streams {
					signalVal = append(signalVal, stream)
				}
				signal = b.signal
			}

		case update, ok := <-b.updates:
			if !ok {
				return
			}
			if update.t.IsZero() {
				delete(active, update.stream)
				break
			}
			active[update.stream] = update.t
			if trigger == nil {
				trigger = b.c.At(update.t.Add(b.max))
			}

		case signal <- signalVal:
			signalVal = nil
			signal = nil
		}
	}
}

// Update lets the StreamReminder know that stream exists, and to include it in future signals if
// it is not updated regularly.
func (b *StreamReminder) Update(stream StreamId) {
	b.updates <- updateItem{stream, b.c.Now()}
}

// Clear removes any knowledge of stream from the StreamReminder.  It's possible for the next signal
// to include stream.
func (b *StreamReminder) Clear(stream StreamId) {
	b.updates <- updateItem{stream, time.Time{}}
}

// Wait returns a channel that signaled streams will be sent on.
func (b *StreamReminder) Wait() <-chan []StreamId {
	return b.signal
}

// Close shuts down the StreamReminder and cleans up the associated goroutine.
func (b *StreamReminder) Close() {
	close(b.updates)
}
