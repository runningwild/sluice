package core_test

import (
	"github.com/runningwild/cmwc"
	"math/rand"

	"github.com/runningwild/sluice/core"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestChunkMergers(t *testing.T) {
	Convey("ChunkMergers work properly.", t, func() {
		Convey("UnreliableUnorderedChunkMergers work properly", func() {
			chunks := []core.Chunk{
				// 3: "ABCD"
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

				// 7: "abcdefghk"
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

				// 12: "123"
				core.Chunk{
					Sequence:    12,
					Subsequence: 0,
					Data:        []byte("123"),
				},
			}
			Convey("when chunks come in order", func() {
				cm := core.MakeUnreliableUnorderedChunkMerger(10)
				packets := cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCD")

				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
			})

			Convey("when chunks come out of order", func() {
				cm := core.MakeUnreliableUnorderedChunkMerger(10)
				packets := cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCD")

				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
			})

			Convey("when duplicate chunks show up", func() {
				cm := core.MakeUnreliableUnorderedChunkMerger(10)
				packets := cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCD")

				// Repeat that packet, shouldn't get anything back.
				packets = cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)
			})

			Convey("when old chunks show up", func() {
				cm := core.MakeUnreliableUnorderedChunkMerger(2)
				packets := cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)

				// This makes all of the other chunks look old, we shouldn't get back anything for them
				// now.
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)

				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)

			})
		})

		Convey("UnreliableOrderedChunkMergers work properly", func() {
			chunks := []core.Chunk{
				// 3: "ABCD"
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

				// 7: "abcdefghk"
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

				// 12: "123"
				core.Chunk{
					Sequence:    12,
					Subsequence: 0,
					Data:        []byte("123"),
				},
			}
			Convey("when chunks come in order", func() {
				cm := core.MakeUnreliableOrderedChunkMerger(100)
				packets := cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCD")

				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
			})

			Convey("when chunks come out of order", func() {
				cm := core.MakeUnreliableOrderedChunkMerger(100)
				packets := cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCD")

				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
			})

			Convey("when packets come out of order", func() {
				cm := core.MakeUnreliableOrderedChunkMerger(100)
				packets := cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				// Finish the first packet, we shouldn't get it.
				packets = cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
			})

			Convey("when duplicate chunks show up", func() {
				cm := core.MakeUnreliableOrderedChunkMerger(100)
				packets := cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCD")

				// Repeat that packet, shouldn't get anything back.
				packets = cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)
			})

			Convey("when old chunks show up", func() {
				cm := core.MakeUnreliableOrderedChunkMerger(3)
				packets := cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)

				// This makes all of the other chunks look old, we shouldn't get back anything for them
				// now.
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)

				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)

			})
		})

		Convey("ReliableUnorderedChunkMergers work properly", func() {
			chunks := []core.Chunk{
				// 3: "ABCDEFGHIJ"
				core.Chunk{
					Sequence:    3,
					Subsequence: 1,
					Data:        []byte("ABC"),
				},
				core.Chunk{
					Sequence:    4,
					Subsequence: 2,
					Data:        []byte("DEF"),
				},
				core.Chunk{
					Sequence:    5,
					Subsequence: 3,
					Data:        []byte("GHI"),
				},
				core.Chunk{
					Sequence:    6,
					Subsequence: 4,
					Data:        []byte("J"),
				},

				// 7: "abcdefghk"
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

				// 11: "123"
				core.Chunk{
					Sequence:    11,
					Subsequence: 0,
					Data:        []byte("123"),
				},

				// 12: "456"
				core.Chunk{
					Sequence:    12,
					Subsequence: 0,
					Data:        []byte("456"),
				},

				// 1000: "FUTURE"
				core.Chunk{
					Sequence:    1000,
					Subsequence: 1,
					Data:        []byte("FUT"),
				},
				core.Chunk{
					Sequence:    1001,
					Subsequence: 2,
					Data:        []byte("URE"),
				},
				core.Chunk{
					Sequence:    1002,
					Subsequence: 3,
					Data:        []byte(""),
				},
			}
			Convey("when chunks come in order", func() {
				cm := core.MakeReliableUnorderedChunkMerger(3)
				packets := cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")

				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = cm.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")

				packets = cm.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "456")
			})

			Convey("when chunks come out of order", func() {
				cm := core.MakeReliableUnorderedChunkMerger(3)
				packets := cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")

				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = cm.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "456")

				packets = cm.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
			})

			Convey("when packets come out of order", func() {
				cm := core.MakeReliableUnorderedChunkMerger(3)

				packets := cm.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "456")

				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")

				packets = cm.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
			})

			Convey("when duplicate chunks show up", func() {
				cm := core.MakeReliableUnorderedChunkMerger(3)
				packets := cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")

				// Resend every chunk in that packet, shouldn't get anything back.
				packets = cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
				packets = cm.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "456")
				packets = cm.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 0)
			})

			Convey("when old chunks show up", func() {
				cm := core.MakeReliableUnorderedChunkMerger(3)
				packets := cm.AddChunk(chunks[10])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[11])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[12])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "FUTURE")

				// Now send some super-old packets that we haven't gotten yet.
				packets = cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")

				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = cm.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")

				packets = cm.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "456")
			})
		})

		Convey("ReliableOrderedChunkMergers work properly", func() {
			chunks := []core.Chunk{
				// 3: "ABCDEFGHIJ"
				core.Chunk{
					Sequence:    3,
					Subsequence: 1,
					Data:        []byte("ABC"),
				},
				core.Chunk{
					Sequence:    4,
					Subsequence: 2,
					Data:        []byte("DEF"),
				},
				core.Chunk{
					Sequence:    5,
					Subsequence: 3,
					Data:        []byte("GHI"),
				},
				core.Chunk{
					Sequence:    6,
					Subsequence: 4,
					Data:        []byte("J"),
				},

				// 7: "abcdefghk"
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

				// 11: "123"
				core.Chunk{
					Sequence:    11,
					Subsequence: 0,
					Data:        []byte("123"),
				},

				// 12: "456"
				core.Chunk{
					Sequence:    12,
					Subsequence: 0,
					Data:        []byte("456"),
				},

				// 1000: "FUTURE"
				core.Chunk{
					Sequence:    1000,
					Subsequence: 1,
					Data:        []byte("FUT"),
				},
				core.Chunk{
					Sequence:    1001,
					Subsequence: 2,
					Data:        []byte("URE"),
				},
				core.Chunk{
					Sequence:    1002,
					Subsequence: 3,
					Data:        []byte(""),
				},
			}
			Convey("when chunks come in order", func() {
				cm := core.MakeReliableOrderedChunkMerger(3)
				packets := cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")

				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = cm.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")

				packets = cm.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "456")
			})

			Convey("when chunks come out of order", func() {
				cm := core.MakeReliableOrderedChunkMerger(3)
				packets := cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")

				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = cm.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")

				packets = cm.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "456")
			})

			Convey("when packets come out of order", func() {
				cm := core.MakeReliableOrderedChunkMerger(3)

				packets := cm.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 0)
				// So(string(packets[0]), ShouldEqual, "456")

				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)
				// So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 4)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")
				So(string(packets[1]), ShouldEqual, "abcdefghk")
				So(string(packets[2]), ShouldEqual, "123")
				So(string(packets[3]), ShouldEqual, "456")
			})

			Convey("when duplicate chunks show up", func() {
				cm := core.MakeReliableOrderedChunkMerger(3)
				packets := cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")

				// Resend every chunk in that packet, shouldn't get anything back.
				packets = cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
				packets = cm.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 0)

				packets = cm.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "456")
				packets = cm.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 0)
			})

			Convey("when old chunks show up", func() {
				cm := core.MakeReliableOrderedChunkMerger(3)
				packets := cm.AddChunk(chunks[10])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[11])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[12])
				So(len(packets), ShouldEqual, 0)

				// Now send some super-old packets that we haven't gotten yet.
				packets = cm.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")

				packets = cm.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)
				packets = cm.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = cm.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")

				packets = cm.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "456")

				// Didn't get "FUTURE" because it is at sequence 1000, far ahead of these.
			})
		})
	})
}

var smallPackets []core.Chunk
var largePackets []core.Chunk

func init() {
	smallPackets = []core.Chunk{
		core.Chunk{
			Subsequence: 1,
			Data:        []byte("..."),
		},
		core.Chunk{
			Subsequence: 2,
			Data:        []byte("..."),
		},
		core.Chunk{
			Subsequence: 3,
			Data:        []byte{},
		},
	}
	for i := 0; i < 100; i++ {
		largePackets = append(largePackets, core.Chunk{
			Subsequence: core.SubsequenceIndex(i + 1),
			Data:        []byte("..."),
		})
	}
	largePackets[99].Data = nil
}

func benchmarkChunkMergerWithInOrderChunks(b *testing.B, merger core.ChunkMerger) {
	b.StopTimer()
	chunks := make([]core.Chunk, b.N)
	for i := range chunks {
		chunks[i] = smallPackets[i%len(smallPackets)]
		chunks[i].Sequence = core.SequenceId(i)
	}
	b.StartTimer()
	for i := range chunks {
		merger.AddChunk(chunks[i])
	}
}

func BenchmarkUnreliableUnorderedChunkMergerWithInOrderChunks(b *testing.B) {
	benchmarkChunkMergerWithInOrderChunks(b, core.MakeUnreliableUnorderedChunkMerger(10))
}

func BenchmarkReliableUnorderedChunkMergerWithInOrderChunks(b *testing.B) {
	benchmarkChunkMergerWithInOrderChunks(b, core.MakeReliableUnorderedChunkMerger(0))
}

func BenchmarkUnreliableOrderedChunkMergerWithInOrderChunks(b *testing.B) {
	benchmarkChunkMergerWithInOrderChunks(b, core.MakeUnreliableOrderedChunkMerger(10))
}

func BenchmarkReliableOrderedChunkMergerWithInOrderChunks(b *testing.B) {
	benchmarkChunkMergerWithInOrderChunks(b, core.MakeReliableOrderedChunkMerger(0))
}

func benchmarkChunkMergerWithOutOfOrderChunks(b *testing.B, merger core.ChunkMerger) {
	b.StopTimer()
	chunks := make([]core.Chunk, b.N)
	for i := range chunks {
		chunks[i] = largePackets[i%len(largePackets)]
		chunks[i].Sequence = core.SequenceId(i)
	}
	c := cmwc.MakeGoodCmwc()
	c.Seed(123)
	rng := rand.New(c)
	// Shuffle blocks of 1000 at a time
	for i := 0; i < len(chunks); i += 1000 {
		max := len(chunks)
		if max > 1000 {
			max = 1000
		}
		for j := 0; j < max; j++ {
			swap := rng.Intn(max-j) + j
			chunks[j], chunks[swap] = chunks[swap], chunks[j]
		}
	}
	b.StartTimer()
	for i := range chunks {
		merger.AddChunk(chunks[i])
	}
}

func BenchmarkUnreliableUnorderedChunkMergerWithOutOfOrderChunks(b *testing.B) {
	benchmarkChunkMergerWithOutOfOrderChunks(b, core.MakeUnreliableUnorderedChunkMerger(10))
}

func BenchmarkReliableUnorderedChunkMergerWithOutOfOrderChunks(b *testing.B) {
	benchmarkChunkMergerWithOutOfOrderChunks(b, core.MakeReliableUnorderedChunkMerger(0))
}

func BenchmarkUnreliableOrderedChunkMergerWithOutOfOrderChunks(b *testing.B) {
	benchmarkChunkMergerWithOutOfOrderChunks(b, core.MakeUnreliableOrderedChunkMerger(10))
}

func BenchmarkReliableOrderedChunkMergerWithOutOfOrderChunks(b *testing.B) {
	benchmarkChunkMergerWithOutOfOrderChunks(b, core.MakeReliableOrderedChunkMerger(0))
}
