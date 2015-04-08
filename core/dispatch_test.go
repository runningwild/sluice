package core_test

import (
	"fmt"
	"github.com/runningwild/clock"
	"github.com/runningwild/cmwc"
	"github.com/runningwild/network"
	"math/rand"
	"time"

	"github.com/runningwild/sluice/core"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

type fakeBlockingConn struct {
	in, out    chan []byte
	dispatches [][]byte
	dropFrac   float64
	rng        *rand.Rand
}

func makeFakeBlockingConn(dropFrac float64) *fakeBlockingConn {
	var fbc fakeBlockingConn
	fbc.in = make(chan []byte)
	fbc.out = make(chan []byte)
	fbc.dropFrac = dropFrac
	c := cmwc.MakeGoodCmwc()
	c.Seed(123)
	fbc.rng = rand.New(c)
	go fbc.run()
	return &fbc
}
func (fbc *fakeBlockingConn) run() {
	var dispatches [][]byte
	var out chan []byte
	var outDispatch []byte
	defer func() {
		if outDispatch != nil {
			fbc.out <- outDispatch
		}
		for _, dispatch := range dispatches {
			fbc.out <- dispatch
		}
	}()
	for {
		select {
		case p, ok := <-fbc.in:
			if !ok {
				return
			}
			if len(dispatches) == 0 {
				out = fbc.out
				outDispatch = p
			}
			dispatches = append(dispatches, p)

		case out <- outDispatch:
			dispatches = dispatches[1:]
			if len(dispatches) == 0 {
				out = nil
			} else {
				outDispatch = dispatches[0]
			}
		}
	}
}
func (fbc *fakeBlockingConn) Write(data []byte) (n int, err error) {
	if fbc.rng.Float64() < fbc.dropFrac {
		return len(data), nil
	}
	b := make([]byte, len(data))
	copy(b, data)
	fbc.in <- b
	return len(data), nil
}
func (fbc *fakeBlockingConn) Read(data []byte) (n int, err error) {
	buf := <-fbc.out
	copy(data, buf)
	n = len(data)
	if len(buf) < n {
		n = len(buf)
	}
	return n, nil
}
func (fbc *fakeBlockingConn) ReadFrom(data []byte) (n int, addr network.Addr, err error) {
	n, err = fbc.Read(data)
	return n, fbc, err
}
func (fbc *fakeBlockingConn) Network() string {
	return "fakeBlockingConn"
}
func (fbc *fakeBlockingConn) String() string {
	return fmt.Sprintf("FBC:%p", fbc)
}
func (fbc *fakeBlockingConn) Close() error {
	close(fbc.in)
	return nil
}

func areChunksEqual(a, b *core.Chunk) bool {
	if a.Source != b.Source {
		return false
	}
	if a.Target != b.Target {
		return false
	}
	if a.Stream != b.Stream {
		return false
	}
	if a.Sequence != b.Sequence {
		return false
	}
	return string(a.Data) == string(b.Data)
}

func TestSerializeAndParseChunks(t *testing.T) {
	chunks := []core.Chunk{
		core.Chunk{
			Source:   2,
			Target:   5,
			Stream:   100,
			Sequence: 3,
			Data:     []byte("I am a thunder gun"),
		},
		core.Chunk{
			Source:   112,
			Target:   52,
			Stream:   1030,
			Sequence: 1122,
			Data:     []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		},
		core.Chunk{
			Source:   23,
			Target:   5,
			Stream:   100,
			Sequence: 33333,
			Data:     []byte(""),
		},
		core.Chunk{
			Source:   0,
			Target:   0,
			Stream:   0,
			Sequence: 0,
			Data:     []byte("A"),
		},
	}

	Convey("Chunks know their serialized length without having to serialize.", t, func() {
		for _, chunk := range chunks {
			So(chunk.SerializedLength(), ShouldEqual, len(core.AppendChunk(nil, &chunk)))
		}
	})

	Convey("Serialized chunks get batched together after the appropriate timeout.", t, func() {
		var serializedData []byte
		// All of this setup is so that we can send all of our chunks and get them serialized
		// into a single send along conn.
		chunksChan := make(chan core.Chunk)
		conn := makeFakeBlockingConn(0)
		defer conn.Close()
		c := &clock.FakeClock{}
		go core.BatchAndSend(chunksChan, conn, c, 10000000, 1000)

		for _, chunk := range chunks {
			chunksChan <- chunk
		}
		c.Inc(time.Millisecond * 10000)
		serializedData = make([]byte, 100000)
		n, err := conn.Read(serializedData)
		So(err, ShouldBeNil)
		serializedData = serializedData[0:n]

		Convey("Then if that data arrives in-tact it should parse correctly.", func() {
			parsed, err := core.ParseChunks(serializedData)
			So(err, ShouldBeNil)
			So(len(parsed), ShouldEqual, len(chunks))
			for i := range chunks {
				So(parsed[i].Source, ShouldEqual, chunks[i].Source)
				So(parsed[i].Target, ShouldEqual, chunks[i].Target)
				So(parsed[i].Stream, ShouldEqual, chunks[i].Stream)
				So(parsed[i].Sequence, ShouldEqual, chunks[i].Sequence)
				So(string(parsed[i].Data), ShouldEqual, string(chunks[i].Data))
			}
		})

		Convey("Then if that data arrives corrupted it should fail to parse.", func() {
			for i := range serializedData {
				serializedData[i]++
				parsed, err := core.ParseChunks(serializedData)
				So(parsed, ShouldBeNil)
				So(err, ShouldNotBeNil)
				serializedData[i]--
			}
		})
	})

	Convey("Sending chunks through BatchAndSend and piping that to ReceiveAndSplit should result in the original chunks.", t, func() {
		chunksIn := make(chan core.Chunk)
		chunksOut := make(chan core.Chunk)
		conn := makeFakeBlockingConn(0)
		defer conn.Close()
		c := &clock.FakeClock{}
		go core.BatchAndSend(chunksIn, conn, c, -1, -1)
		go core.ReceiveAndSplit(conn, chunksOut, 100000)
		go func() {
			for _, chunk := range chunks {
				chunksIn <- chunk
			}
			close(chunksIn)
		}()
		found := make(map[int]bool)
		for len(found) < len(chunks) {
			chunk := <-chunksOut
			for i := range chunks {
				if areChunksEqual(&chunk, &chunks[i]) {
					So(found[i], ShouldBeFalse)
					found[i] = true
				}
			}
		}
		So(len(found), ShouldEqual, len(chunks))
	})
}
