package core_test

import (
	"github.com/runningwild/sluice/core"

	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func checkSequenceTracker(st *core.SequenceTracker, max core.SequenceId, sids map[core.SequenceId]bool) bool {
	good := true
	containedUpToMax := st.ContainsAllUpTo(max)
	So(containedUpToMax, ShouldBeTrue)
	if !containedUpToMax {
		good = false
	}
	doesntContainNext := st.ContainsAllUpTo(max + 1)
	So(doesntContainNext, ShouldBeFalse)
	if !doesntContainNext {
		good = false
	}
	for sid, present := range sids {
		So(st.Contains(sid), ShouldEqual, present)
		if st.Contains(sid) != present {
			good = false
		}
	}
	return good
}

func SequenceTrackerTest(t *testing.T) {
	st := core.MakeSequenceTracker(2345, 0, 10)
	Convey("Everything under maxContiguous is contained by the tracker.", t, func() {
		So(st.StreamId(), ShouldEqual, core.StreamId(2345))
		So(st.Contains(1), ShouldBeTrue)
		So(st.Contains(2), ShouldBeTrue)
		So(st.Contains(4), ShouldBeTrue)
		So(st.Contains(8), ShouldBeTrue)
		So(st.Contains(10), ShouldBeTrue)
		So(st.Contains(11), ShouldBeTrue)
		So(st.Contains(14), ShouldBeTrue)
		So(st.Contains(17), ShouldBeTrue)
		So(st.Contains(100), ShouldBeTrue)
		So(st.Contains(1000), ShouldBeTrue)
	})

	Convey("Scattered sequence ids are identified.", t, func() {
		st.AddSequenceId(12)
		st.AddSequenceId(13)
		st.AddSequenceId(14)
		st.AddSequenceId(20)
		st.AddSequenceId(22)
		sids := map[core.SequenceId]bool{
			10: false,
			11: false,
			12: true,
			13: true,
			14: true,
			15: false,
			16: false,
			17: false,
			18: false,
			19: false,
			20: true,
			21: false,
			22: true,
			23: false,
			24: false,
		}
		So(st.StreamId(), ShouldEqual, core.StreamId(2345))
		checkSequenceTracker(st, 9, sids)
		Convey("Serialize() followed by Deserialize() is a NOP.", func() {
			var data []byte
			data = core.AppendSequenceTracker(data, st)
			var st2 core.SequenceTracker
			var err error
			data, err = core.ConsumeSequenceTracker(data, &st2)
			So(len(data), ShouldEqual, 0)
			So(err, ShouldBeNil)
			So(st2.StreamId(), ShouldEqual, core.StreamId(2345))
			checkSequenceTracker(&st2, 9, sids)
		})
	})

	Convey("Serialization is correct after compaction.", t, func() {
		st.AddSequenceId(12)
		st.AddSequenceId(14)
		st.AddSequenceId(15)
		st.AddSequenceId(10)
		st.AddSequenceId(11) // Should compact up to 11
		st.AddSequenceId(13) // Should compact up to 15
		var data []byte
		data = core.AppendSequenceTracker(data, st)

		// streamid + len + sequence id == 2 + 2 + 2 + 4 == 10
		So(len(data), ShouldEqual, 10)

		checkSequenceTracker(st, 15, map[core.SequenceId]bool{
			10: true,
			11: true,
			12: true,
			13: true,
			14: true,
			15: true,
			16: false,
			17: false,
			18: false,
		})
	})
}
