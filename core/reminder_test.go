package core_test

import (
	"time"

	"github.com/runningwild/clock"
	"github.com/runningwild/sluice/core"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestRemindersSignalBatcher(t *testing.T) {
	Convey("StreamReminder", t, func() {
		fc := &clock.FakeClock{}
		fc.Inc(time.Millisecond)
		r := core.MakeStreamReminder(10*time.Millisecond, 20*time.Millisecond, fc)
		Convey("Will signal streams that havne't been updated recently", func() {
			r.Update(1)
			r.Update(2)
			r.Update(3)
			fc.Inc(25 * time.Millisecond)
			vals := <-r.Wait()
			So(vals, ShouldContain, 1)
			So(vals, ShouldContain, 2)
			So(vals, ShouldContain, 3)
		})
		Convey("Won't signal as long as all streams are updated regularly.", func() {
			r.Update(1)
			fc.Inc(3 * time.Millisecond)
			r.Update(2)
			fc.Inc(3 * time.Millisecond)
			r.Update(3)
			fc.Inc(3 * time.Millisecond)
			r.Update(1)
			fc.Inc(3 * time.Millisecond)
			r.Update(2)
			fc.Inc(3 * time.Millisecond)
			r.Update(3)
			fc.Inc(3 * time.Millisecond)
			r.Update(1)
			fc.Inc(3 * time.Millisecond)
			r.Update(2)
			fc.Inc(3 * time.Millisecond)
			r.Update(3)
			fc.Inc(3 * time.Millisecond)
			select {
			case vals := <-r.Wait():
				t.Errorf("Should not have gotten a value from r.Wait(), got %v", vals)
			default:
			}
		})

		Convey("Will signal a stream that has not been updated recently.", func() {
			r.Update(1)
			fc.Inc(3 * time.Millisecond)
			r.Update(2)
			fc.Inc(3 * time.Millisecond)
			r.Update(3)
			fc.Inc(3 * time.Millisecond)
			r.Update(2)
			fc.Inc(3 * time.Millisecond)
			r.Update(3)
			fc.Inc(3 * time.Millisecond)
			r.Update(2)
			fc.Inc(3 * time.Millisecond)
			r.Update(3)
			fc.Inc(3 * time.Millisecond)
			vals := <-r.Wait()
			So(vals, ShouldResemble, []core.StreamId{1})
		})

		Convey("Will not signal a stream that is continuously updated.", func() {
			r.Update(1)
			fc.Inc(3 * time.Millisecond)
			r.Update(2)
			fc.Inc(3 * time.Millisecond)
			r.Update(3)
			fc.Inc(3 * time.Millisecond)
			r.Update(3)
			fc.Inc(3 * time.Millisecond)
			r.Update(3)
			fc.Inc(3 * time.Millisecond)
			r.Update(3)
			fc.Inc(3 * time.Millisecond)
			r.Update(3)
			fc.Inc(3 * time.Millisecond)
			r.Update(3)
			fc.Inc(3 * time.Millisecond)
			r.Update(3)
			vals := <-r.Wait()
			So(len(vals), ShouldEqual, 2)
			So(vals, ShouldContain, 1)
			So(vals, ShouldContain, 2)
			fc.Inc(30 * time.Millisecond)
			vals = <-r.Wait()
			So(len(vals), ShouldEqual, 3)
			So(vals, ShouldContain, 1)
			So(vals, ShouldContain, 2)
			So(vals, ShouldContain, 3)
		})

		Convey("Can clear streams (FLAKY!!!).", func() {
			for i := 0; i < 5; i++ {
				r.Update(1)
				fc.Inc(3 * time.Millisecond)
				r.Update(2)
				fc.Inc(3 * time.Millisecond)
				r.Update(3)
				fc.Inc(3 * time.Millisecond)
			}
			fc.Inc(30 * time.Millisecond)
			time.Sleep(time.Millisecond)
			vals := <-r.Wait()
			So(len(vals), ShouldEqual, 3)
			So(vals, ShouldContain, 1)
			So(vals, ShouldContain, 2)
			So(vals, ShouldContain, 3)

			fc.Inc(30 * time.Millisecond)
			time.Sleep(time.Millisecond)
			vals = <-r.Wait()
			So(len(vals), ShouldEqual, 3)
			So(vals, ShouldContain, 1)
			So(vals, ShouldContain, 2)
			So(vals, ShouldContain, 3)

			r.Clear(1)
			r.Clear(2)
			fc.Inc(30 * time.Millisecond)
			time.Sleep(time.Millisecond)
			vals = <-r.Wait()
			So(len(vals), ShouldEqual, 1)
			So(vals, ShouldContain, 3)

			fc.Inc(30 * time.Millisecond)
			time.Sleep(time.Millisecond)
			vals = <-r.Wait()
			So(len(vals), ShouldEqual, 1)
			So(vals, ShouldContain, 3)
		})
	})
}
