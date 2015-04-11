package reminder_test

import (
	"time"

	"github.com/runningwild/clock"
	"github.com/runningwild/sluice/core/reminder"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestRemindersSignalBatcher(t *testing.T) {
	Convey("Reminder.Batch triggers on single events.", t, func() {
		fc := &clock.FakeClock{}
		r := reminder.MakeBatchReminderWithClock(fc)
		defer r.Close()
		r.Ping(time.Millisecond)
		fc.Inc(2 * time.Millisecond)
		So(<-r.Wait(), ShouldEqual, 1)
	})
	Convey("Reminder.Batch triggers once for a continuous stream of events.", t, func() {
		fc := &clock.FakeClock{}
		r := reminder.MakeBatchReminderWithClock(fc)
		defer r.Close()
		pings := 100
		for i := 0; i < pings; i++ {
			r.Ping(2 * time.Millisecond)
			fc.Inc(time.Millisecond)
			select {
			case <-r.Wait():
				t.Error("Reminder should not have triggered.")
			default:
			}
		}
		fc.Inc(10 * time.Millisecond)
		So(<-r.Wait(), ShouldEqual, pings)
	})
	Convey("Reminder.Batch triggers multiple times for multiple streams.", t, func() {
		fc := &clock.FakeClock{}
		r := reminder.MakeBatchReminderWithClock(fc)
		defer r.Close()
		for j := 0; j < 5; j++ {
			pings := 100
			for i := 0; i < pings; i++ {
				r.Ping(2 * time.Millisecond)
				fc.Inc(time.Millisecond)
				select {
				case <-r.Wait():
					t.Error("Reminder should not have triggered.")
				default:
				}
			}
			fc.Inc(10 * time.Millisecond)
			So(<-r.Wait(), ShouldEqual, pings)
		}
	})
}
