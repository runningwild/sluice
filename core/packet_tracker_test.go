package core_test

import (
	"github.com/runningwild/sluice/core"

	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

// makeSimpleChunk fills out the data field of the chunk with the stream/node/sequence data so we
// can verify it later.
func makeSimpleChunk(stream core.StreamId, node core.NodeId, sequence core.SequenceId) core.Chunk {
	var data []byte
	data = core.AppendStreamId(data, stream)
	data = core.AppendNodeId(data, node)
	data = core.AppendSequenceId(data, sequence)
	return core.Chunk{
		Stream:   stream,
		Source:   node,
		Sequence: sequence,
		Data:     data,
	}
}

func verifySimpleChunk(chunk *core.Chunk) bool {
	if chunk == nil {
		return false
	}
	var stream core.StreamId
	var node core.NodeId
	var sequence core.SequenceId
	data := chunk.Data
	data = core.ConsumeStreamId(data, &stream)
	data = core.ConsumeNodeId(data, &node)
	data = core.ConsumeSequenceId(data, &sequence)
	return chunk.Stream == stream && chunk.Source == node && chunk.Sequence == sequence
}

func TestPacketTracker(t *testing.T) {
	Convey("PacketTracker", t, func() {
		pt := make(core.PacketTracker)
		Convey("Shouldn't claim to have any chunks when it's empty.", func() {
			So(pt.Contains(1, 1, 1), ShouldBeFalse)
			So(pt.Contains(2, 2, 2), ShouldBeFalse)
			So(pt.Contains(10, 3, 3), ShouldBeFalse)
			So(pt.ContainsAnyFor(1, 1), ShouldBeFalse)
			So(pt.ContainsAnyFor(1, 2), ShouldBeFalse)
			So(pt.ContainsAnyFor(2, 1), ShouldBeFalse)
			So(pt.ContainsAnyFor(2, 2), ShouldBeFalse)
		})
		pt.Add(makeSimpleChunk(1, 1, 10))
		pt.Add(makeSimpleChunk(1, 1, 11))
		pt.Add(makeSimpleChunk(1, 1, 12))
		pt.Add(makeSimpleChunk(2, 3, 30))
		pt.Add(makeSimpleChunk(3, 3, 100))
		Convey("Knows which chunks it has.", func() {
			So(pt.ContainsAnyFor(1, 1), ShouldBeTrue)
			So(pt.ContainsAnyFor(2, 3), ShouldBeTrue)
			So(pt.ContainsAnyFor(3, 3), ShouldBeTrue)
			So(pt.ContainsAnyFor(2, 1), ShouldBeFalse)
			So(pt.ContainsAnyFor(3, 1), ShouldBeFalse)
			So(pt.ContainsAnyFor(1, 3), ShouldBeFalse)
			So(pt.Contains(1, 1, 10), ShouldBeTrue)
			So(pt.Contains(1, 1, 11), ShouldBeTrue)
			So(pt.Contains(1, 1, 12), ShouldBeTrue)
			So(pt.Contains(2, 3, 30), ShouldBeTrue)
			So(pt.Contains(3, 3, 100), ShouldBeTrue)
			So(pt.Contains(1, 2, 10), ShouldBeFalse)
			So(pt.Contains(1, 3, 20), ShouldBeFalse)
			So(pt.Contains(3, 1, 30), ShouldBeFalse)
		})
		Convey("Can return the chunks it's been given.", func() {
			So(pt.Get(1, 1, 10), ShouldNotBeNil)
			So(pt.Get(1, 1, 11), ShouldNotBeNil)
			So(pt.Get(1, 1, 12), ShouldNotBeNil)
			So(pt.Get(2, 3, 30), ShouldNotBeNil)
			So(pt.Get(3, 3, 100), ShouldNotBeNil)
			So(verifySimpleChunk(pt.Get(1, 1, 10)), ShouldBeTrue)
			So(verifySimpleChunk(pt.Get(1, 1, 11)), ShouldBeTrue)
			So(verifySimpleChunk(pt.Get(1, 1, 12)), ShouldBeTrue)
			So(verifySimpleChunk(pt.Get(2, 3, 30)), ShouldBeTrue)
			So(pt.Get(3, 3, 100), ShouldNotBeNil)
			So(pt.Get(1, 2, 10), ShouldBeNil)
			So(pt.Get(1, 3, 20), ShouldBeNil)
			So(pt.Get(3, 1, 30), ShouldBeNil)
		})
		pt.Add(makeSimpleChunk(1, 1, 13))
		pt.Add(makeSimpleChunk(1, 1, 14))
		pt.Add(makeSimpleChunk(1, 1, 15))
		pt.Add(makeSimpleChunk(1, 1, 16))
		Convey("Can remove chunks.", func() {
			pt.Remove(1, 1, 10)
			pt.Remove(1, 1, 12)
			So(pt.Contains(1, 1, 10), ShouldBeFalse)
			So(pt.Contains(1, 1, 11), ShouldBeTrue)
			So(pt.Contains(1, 1, 12), ShouldBeFalse)
			So(pt.Contains(1, 1, 13), ShouldBeTrue)
			So(pt.ContainsAnyFor(1, 1), ShouldBeTrue)
			pt.Remove(1, 1, 11)
			pt.Remove(1, 1, 13)
			pt.Remove(1, 1, 14)
			pt.Remove(1, 1, 15)
			pt.Remove(1, 1, 16)
			So(pt.Contains(1, 1, 10), ShouldBeFalse)
			So(pt.Contains(1, 1, 11), ShouldBeFalse)
			So(pt.Contains(1, 1, 12), ShouldBeFalse)
			So(pt.Contains(1, 1, 13), ShouldBeFalse)
			So(pt.Contains(1, 1, 14), ShouldBeFalse)
			So(pt.Contains(1, 1, 15), ShouldBeFalse)
			So(pt.Contains(1, 1, 16), ShouldBeFalse)
			So(pt.ContainsAnyFor(1, 1), ShouldBeFalse)
		})
		Convey("Can truncate chunks.", func() {
			So(pt.ContainsAnyFor(1, 1), ShouldBeTrue)
			pt.RemoveUpToAndIncluding(1, 1, 12)
			So(pt.ContainsAnyFor(1, 1), ShouldBeTrue)
			So(pt.Contains(1, 1, 10), ShouldBeFalse)
			So(pt.Contains(1, 1, 11), ShouldBeFalse)
			So(pt.Contains(1, 1, 12), ShouldBeFalse)
			So(pt.Contains(1, 1, 13), ShouldBeTrue)
			pt.RemoveUpToAndIncluding(1, 1, 20)
			So(pt.ContainsAnyFor(1, 1), ShouldBeFalse)
		})
		st := core.MakeSequenceTracker(1, 1, 12)
		st.AddSequenceId(14)
		st.AddSequenceId(15)
		Convey("Can remove chunks based on a sequence tracker.", func() {
			pt.RemoveSequenceTracked(st)
			So(pt.Contains(1, 1, 10), ShouldBeFalse)
			So(pt.Contains(1, 1, 11), ShouldBeFalse)
			So(pt.Contains(1, 1, 12), ShouldBeTrue)
			So(pt.Contains(1, 1, 13), ShouldBeTrue)
			So(pt.Contains(1, 1, 14), ShouldBeFalse)
			So(pt.Contains(1, 1, 15), ShouldBeFalse)
			So(pt.Contains(1, 1, 16), ShouldBeTrue)
		})
	})
}
