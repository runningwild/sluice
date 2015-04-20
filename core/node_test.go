package core_test

import (
	"github.com/runningwild/cmwc"
	"math/rand"

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
				ct := core.MakeReliableUnorderedChunkMerger(3)
				packets := ct.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")

				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = ct.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")

				packets = ct.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "456")
			})

			Convey("when chunks come out of order", func() {
				ct := core.MakeReliableUnorderedChunkMerger(3)
				packets := ct.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")

				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = ct.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "456")

				packets = ct.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
			})

			Convey("when packets come out of order", func() {
				ct := core.MakeReliableUnorderedChunkMerger(3)

				packets := ct.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "456")

				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = ct.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")

				packets = ct.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
			})

			Convey("when duplicate chunks show up", func() {
				ct := core.MakeReliableUnorderedChunkMerger(3)
				packets := ct.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")

				// Resend every chunk in that packet, shouldn't get anything back.
				packets = ct.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 0)

				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")
				packets = ct.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)

				packets = ct.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")
				packets = ct.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 0)

				packets = ct.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "456")
				packets = ct.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 0)
			})

			Convey("when old chunks show up", func() {
				ct := core.MakeReliableUnorderedChunkMerger(3)
				packets := ct.AddChunk(chunks[10])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[11])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[12])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "FUTURE")

				// Now send some super-old packets that we haven't gotten yet.
				packets = ct.AddChunk(chunks[0])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[1])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[2])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[3])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "ABCDEFGHIJ")

				packets = ct.AddChunk(chunks[4])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[5])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[6])
				So(len(packets), ShouldEqual, 0)
				packets = ct.AddChunk(chunks[7])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "abcdefghk")

				packets = ct.AddChunk(chunks[8])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "123")

				packets = ct.AddChunk(chunks[9])
				So(len(packets), ShouldEqual, 1)
				So(string(packets[0]), ShouldEqual, "456")
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

func BenchmarkUnreliableUnorderedChunkMergerWithInOrderChunks(b *testing.B) {
	b.StopTimer()
	chunks := make([]core.Chunk, b.N)
	for i := range chunks {
		chunks[i] = smallPackets[i%len(smallPackets)]
		chunks[i].Sequence = core.SequenceId(i)
	}
	ct := core.MakeUnreliableUnorderedChunkMerger(10)
	b.StartTimer()
	for i := range chunks {
		ct.AddChunk(chunks[i])
	}
}

func BenchmarkReliableUnorderedChunkMergerWithInOrderChunks(b *testing.B) {
	b.StopTimer()
	chunks := make([]core.Chunk, b.N)
	for i := range chunks {
		chunks[i] = smallPackets[i%len(smallPackets)]
		chunks[i].Sequence = core.SequenceId(i)
	}
	ct := core.MakeReliableUnorderedChunkMerger(0)
	b.StartTimer()
	for i := range chunks {
		ct.AddChunk(chunks[i])
	}
}

func BenchmarkUnreliableUnorderedChunkMergerWithOutOfOrderChunks(b *testing.B) {
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
	ct := core.MakeUnreliableUnorderedChunkMerger(10)
	b.StartTimer()
	for i := range chunks {
		ct.AddChunk(chunks[i])
	}
}

func BenchmarkReliableUnorderedChunkMergerWithOutOfOrderChunks(b *testing.B) {
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
	ct := core.MakeReliableUnorderedChunkMerger(10)
	b.StartTimer()
	for i := range chunks {
		ct.AddChunk(chunks[i])
	}
}
