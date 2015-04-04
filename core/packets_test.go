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
	in, out  chan []byte
	packets  [][]byte
	dropFrac float64
	rng      *rand.Rand
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
	var packets [][]byte
	var out chan []byte
	var outPacket []byte
	for {
		select {
		case p := <-fbc.in:
			if len(packets) == 0 {
				out = fbc.out
				outPacket = p
			}
			packets = append(packets, p)

		case out <- outPacket:
			packets = packets[1:]
			if len(packets) == 0 {
				out = nil
			} else {
				outPacket = packets[0]
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

func TestSerializeAndParsePackets(t *testing.T) {
	Convey("Serialized packets get batched together after the appropriate timeout", t, func() {
		packets := []core.Packet{
			core.Packet{
				Source:    2,
				Target:    5,
				Stream:    100,
				Sequenced: true,
				Sequence:  3,
				Data:      []byte("I am a thunder gun"),
			},
			core.Packet{
				Source:    112,
				Target:    52,
				Stream:    1030,
				Sequenced: false,
				Data:      []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			},
			core.Packet{
				Source:    23,
				Target:    5,
				Stream:    100,
				Sequenced: true,
				Sequence:  33333,
				Data:      []byte(""),
			},
			core.Packet{
				Source:    0,
				Target:    0,
				Stream:    0,
				Sequenced: true,
				Sequence:  0,
				Data:      []byte("A"),
			},
		}

		var serializedData []byte
		// All of this setup is so that we can send all of our packets and get them serialized
		// into a single send along conn.
		packetsChan := make(chan core.Packet)
		conn := makeFakeBlockingConn(0)
		c := &clock.FakeClock{}
		go core.BatchAndSend(packetsChan, conn, c, 10000000, 1000)

		for _, packet := range packets {
			packetsChan <- packet
		}
		c.Inc(time.Millisecond * 10000)
		serializedData = make([]byte, 100000)
		n, err := conn.Read(serializedData)
		So(err, ShouldBeNil)
		serializedData = serializedData[0:n]

		Convey("Then if that data arrives in-tact it should parse correctly.", func() {
			parsed, err := core.ParsePackets(serializedData)
			So(err, ShouldBeNil)
			So(len(parsed), ShouldEqual, len(packets))
			for i := range packets {
				So(parsed[i].Source, ShouldEqual, packets[i].Source)
				So(parsed[i].Target, ShouldEqual, packets[i].Target)
				So(parsed[i].Stream, ShouldEqual, packets[i].Stream)
				So(parsed[i].Sequenced, ShouldEqual, packets[i].Sequenced)
				if parsed[i].Sequenced {
					So(parsed[i].Sequence, ShouldEqual, packets[i].Sequence)
				}
				So(string(parsed[i].Data), ShouldEqual, string(packets[i].Data))
			}
		})

		Convey("Then if that data arrives corrupted it should fail to parse.", func() {
			for i := range serializedData {
				serializedData[i]++
				parsed, err := core.ParsePackets(serializedData)
				So(parsed, ShouldBeNil)
				So(err, ShouldNotBeNil)
				serializedData[i]--
			}
		})
	})
}
