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
			`Lorem ipsum dolor sit amet, consecmetur adipiscing elit, sed do eiusmod tempor
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
			cm := core.MakeUnreliableUnorderedChunkMerger(1)
			var reassembled []string
			for _, chunk := range chunks {
				dechunked := cm.AddChunk(chunk)
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
