package core_test

import (
	"github.com/runningwild/sluice/core"

	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func verifySequenceTracker(st *core.SequenceTracker, max core.SequenceId, sids map[core.SequenceId]bool) {
	So(max, ShouldEqual, st.MaxContiguousSequence())
	So(st.Contains(max+1), ShouldBeFalse)
	for sid, present := range sids {
		So(st.Contains(sid), ShouldEqual, present)
	}
}

func SequenceTrackerTest(t *testing.T) {
	st := core.MakeSequenceTracker(2345, 77, 10)
	Convey("It remembers its own stream and node ids.", t, func() {
		So(st.StreamId(), ShouldEqual, 2345)
		So(st.NodeId(), ShouldEqual, 77)
	})
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
		verifySequenceTracker(st, 9, sids)
		Convey("and chunkification/dechunkification works", func() {
			var config core.Config
			config.MaxChunkDataSize = 10
			datas := core.MakeSequenceTrackerChunkDatas(&config, st)
			So(len(datas), ShouldBeGreaterThan, 1)
			var sts []*core.SequenceTracker
			for _, data := range datas {
				st, err := core.ParseSequenceTrackerChunkData(data)
				So(err, ShouldBeNil)
				sts = append(sts, st)
			}

			// All trackers should agree on MaxContiguousSequence.
			So(st.MaxContiguousSequence(), ShouldEqual, 10)
			for i := range sts {
				So(sts[i].MaxContiguousSequence(), ShouldEqual, 10)
			}

			// For each scattered sequence id, at least one tracker should have it.  For sequences
			// not contained in the set, none should have it.
			for sequence, has := range sids {
				found := false
				for _, st := range sts {
					found = found || st.Contains(sequence)
				}
				So(found, ShouldEqual, has)
			}
		})
	})

	Convey("Serialization is correct after compaction.", t, func() {
		st.AddSequenceId(12)
		st.AddSequenceId(14)
		st.AddSequenceId(15)
		st.AddSequenceId(10)
		st.AddSequenceId(11) // Should compact up to 11
		st.AddSequenceId(13) // Should compact up to 15
		sids := map[core.SequenceId]bool{
			10: true,
			11: true,
			12: true,
			13: true,
			14: true,
			15: true,
			16: false,
			17: false,
			18: false,
		}
		verifySequenceTracker(st, 15, sids)

		Convey("and chunkification/dechunkification works", func() {
			var config core.Config
			config.MaxChunkDataSize = 10
			datas := core.MakeSequenceTrackerChunkDatas(&config, st)
			So(len(datas), ShouldBeGreaterThan, 1)
			var sts []*core.SequenceTracker
			for _, data := range datas {
				st, err := core.ParseSequenceTrackerChunkData(data)
				So(err, ShouldBeNil)
				sts = append(sts, st)
			}

			// All trackers should agree on MaxContiguousSequence.
			So(st.MaxContiguousSequence(), ShouldEqual, 10)
			for i := range sts {
				So(sts[i].MaxContiguousSequence(), ShouldEqual, 10)
			}

			// For each scattered sequence id, at least one tracker should have it.  For sequences
			// not contained in the set, none should have it.
			for sequence, has := range sids {
				found := false
				for _, st := range sts {
					found = found || st.Contains(sequence)
				}
				So(found, ShouldEqual, has)
			}
		})
	})
}
