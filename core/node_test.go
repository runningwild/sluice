package core_test

import (
	"github.com/runningwild/sluice/core"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestChunkTrackers(t *testing.T) {
	chunks := []core.Chunk{
		core.Chunk{
			Sequence:    3,
			Subsequence: 1,
			Data:        []byte("ABC"),
		},
		core.Chunk{
			Sequence:    3,
			Subsequence: 2,
			Data:        []byte("D"),
		},
		core.Chunk{
			Sequence:    5,
			Subsequence: 1,
			Data:        []byte("abc"),
		},
		core.Chunk{
			Sequence:    5,
			Subsequence: 2,
			Data:        []byte("def"),
		},
		core.Chunk{
			Sequence:    5,
			Subsequence: 3,
			Data:        []byte("gh"),
		},
		core.Chunk{
			Sequence:    10,
			Subsequence: 0,
			Data:        []byte("123"),
		},
	}
	Convey("ChunkTrackers work properly.", t, func() {
		Convey("UnreliableUnorderedChunkTrackers work properly", func() {
			Convey("when chunks come in order", func() {
				ct := core.MakeUnreliableUnorderedChunkTracker(10)
				packets := ct.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCD")

				packets = ct.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefgh")

				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
			})

			Convey("when chunks come out of order", func() {
				ct := core.MakeUnreliableUnorderedChunkTracker(10)
				packets := ct.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCD")

				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefgh")

				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
			})

			Convey("when duplicate chunks show up", func() {
				ct := core.MakeUnreliableUnorderedChunkTracker(10)
				packets := ct.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCD")

				// Repeat that packet, shouldn't get anything back.
				packets = ct.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)

				packets = ct.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefgh")
				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)

				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
			})

			Convey("when old chunks show up", func() {
				ct := core.MakeUnreliableUnorderedChunkTracker(2)
				packets := ct.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)

				// This makes all of the other chunks look old, we shouldn't get back anything for them
				// now.
				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 1)

				packets = ct.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)

				packets = ct.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)

			})
		})
	})
}
