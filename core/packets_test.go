package core_test

import (
	"github.com/runningwild/sluice/core"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestSerializeAndParsePackets(t *testing.T) {
	Convey("Serialized packets get parsed properly", t, func() {
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
		var buf []byte
		buf = core.SerializePackets(packets, buf)

		Convey("If a packet arrives in-tact it should parse correctly.", func() {
			parsed, err := core.ParsePackets(buf)
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

		Convey("If a packet arrives corrupted it should fail to parse.", func() {
			for i := range buf {
				buf[i]++
				parsed, err := core.ParsePackets(buf)
				So(parsed, ShouldBeNil)
				So(err, ShouldNotBeNil)
				buf[i]--
			}
		})
	})
}
