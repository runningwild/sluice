package core_test

import (
	"github.com/runningwild/sluice/core"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestWriterRoutine(t *testing.T) {
	Convey("WriterRoutine", t, func() {
		packets := []string{
			"I am a short packet.",
			"",
			`Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor
			incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud
			exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure
			dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.
			Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt
			mollit anim id est laborum.`,
		}

		streamConfig := core.StreamConfig{
			Broadcast: false,
			Id:        10,
			Mode:      core.ModeUnreliableUnordered,
			Name:      "testConfig",
		}

		// Want to test packets that are evenly divisible by maxChunkDataSize, so we'll set it to
		// whatever half of the first packet is, but we need to make sure the first packet has even
		// length first.
		So(len(packets[0])%2, ShouldEqual, 0)
		maxChunkDataSize := len(packets[0]) / 2
		packetsIn := make(chan []byte)
		chunksOut := make(chan core.Chunk)
		go func() {
			core.WriterRoutine(streamConfig, 123, maxChunkDataSize, packetsIn, chunksOut)
			close(chunksOut)
		}()
		go func() {
			for _, packet := range packets {
				packetsIn <- []byte(packet)
			}
			close(packetsIn)
		}()
		var chunks []core.Chunk
		for chunk := range chunksOut {
			chunks = append(chunks, chunk)
		}
		Convey("Chunkifies packets into the appropriate size.", func() {
			for _, chunk := range chunks {
				So(len(chunk.Data), ShouldBeLessThanOrEqualTo, maxChunkDataSize)
			}
		})
		Convey("Output is properly reassembled by a ChunkMerger.", func() {
			ct := core.MakeUnreliableUnorderedChunkMerger(1)
			var reassembled []string
			for _, chunk := range chunks {
				dechunked := ct.AddChunk(chunk)
				for _, packet := range dechunked {
					reassembled = append(reassembled, string(packet))
				}
			}
			So(len(reassembled), ShouldEqual, len(packets))
			for i := range reassembled {
				So(reassembled[i], ShouldEqual, packets[i])
			}
		})
	})
}

func TestChunkMergers(t *testing.T) {
	chunks := []core.Chunk{
		core.Chunk{
			Sequence:    3,
			Subsequence: 1,
			Data:        []byte("ABC"),
		},
		core.Chunk{
			Sequence:    4,
			Subsequence: 2,
			Data:        []byte("D"),
		},
		core.Chunk{
			Sequence:    7,
			Subsequence: 1,
			Data:        []byte("abc"),
		},
		core.Chunk{
			Sequence:    8,
			Subsequence: 2,
			Data:        []byte("def"),
		},
		core.Chunk{
			Sequence:    9,
			Subsequence: 3,
			Data:        []byte("ghk"),
		},
		core.Chunk{
			Sequence:    10,
			Subsequence: 4,
			Data:        []byte(""),
		},
		core.Chunk{
			Sequence:    12,
			Subsequence: 0,
			Data:        []byte("123"),
		},
	}
	Convey("ChunkMergers work properly.", t, func() {
		Convey("UnreliableUnorderedChunkMergers work properly", func() {
			Convey("when chunks come in order", func() {
				ct := core.MakeUnreliableUnorderedChunkMerger(10)
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
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = ct.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
			})

			Convey("when chunks come out of order", func() {
				ct := core.MakeUnreliableUnorderedChunkMerger(10)
				packets := ct.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCD")

				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = ct.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
			})

			Convey("when duplicate chunks show up", func() {
				ct := core.MakeUnreliableUnorderedChunkMerger(10)
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
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")
				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)

				packets = ct.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
				packets = ct.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)
			})

			Convey("when old chunks show up", func() {
				ct := core.MakeUnreliableUnorderedChunkMerger(2)
				packets := ct.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)

				// This makes all of the other chunks look old, we shouldn't get back anything for them
				// now.
				packets = ct.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)

				packets = ct.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)

				packets = ct.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)

			})
		})
	})
}
