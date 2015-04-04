package core_test

import (
	"github.com/runningwild/sluice/core"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestSerialization(t *testing.T) {
	Convey("Encoding bools", t, func() {
		var data []byte
		input := []bool{true, false, false, false, true, true, false, true, true, false}
		for _, payload := range input {
			data = core.AppendBool(data, payload)
		}

		var payload bool
		for _, expected := range input {
			data = core.ConsumeBool(data, &payload)
			So(payload, ShouldEqual, expected)
		}

		Convey("All of the data should have been consumed", func() {
			So(len(data), ShouldEqual, 0)
		})
	})

	Convey("Encoding uint16s", t, func() {
		var data []byte
		input := []uint16{0, 1, 2, 4, 8, 16, 32, 65535, 12345, 20000}
		for _, payload := range input {
			data = core.AppendUint16(data, payload)
		}

		var payload uint16
		for _, expected := range input {
			data = core.ConsumeUint16(data, &payload)
			So(payload, ShouldEqual, expected)

		}

		Convey("All of the data should have been consumed", func() {
			So(len(data), ShouldEqual, 0)
		})
	})

	Convey("Encoding uint32s", t, func() {
		var data []byte
		input := []uint32{0, 1, 2, 4, 8, 32, 32, 65535, 1 << 24, 1 << 31, (1<<7 | 1<<15 | 1<<23 | 1<<31)}
		for _, payload := range input {
			data = core.AppendUint32(data, payload)
		}

		var payload uint32
		for _, expected := range input {
			data = core.ConsumeUint32(data, &payload)
			So(payload, ShouldEqual, expected)

		}

		Convey("All of the data should have been consumed", func() {
			So(len(data), ShouldEqual, 0)
		})
	})

	Convey("Encoding strings", t, func() {
		var data []byte
		input := []string{"", "thunder", "foo bar wing ding monkey ball"}
		for _, payload := range input {
			data = core.AppendStringWithLength(data, payload)
		}

		var payload string
		for _, expected := range input {
			var err error
			data, err = core.ConsumeStringWithLength(data, &payload)
			So(err, ShouldBeNil)
			So(payload, ShouldEqual, expected)

		}

		Convey("All of the data should have been consumed", func() {
			So(len(data), ShouldEqual, 0)
		})

		Convey("Improperly encoded strings will returns errors", func() {
			// This test assumes that strings are encoded with 16 bits of length as a prefix.
			var data []byte
			data = core.AppendUint16(data, 10000)
			var payload string
			_, err := core.ConsumeStringWithLength(data, &payload)
			So(err, ShouldNotBeNil)
		})
	})

	Convey("Encoding bytes", t, func() {
		var data []byte
		input := [][]byte{[]byte(""), []byte("thunder"), []byte("foo bar wing ding monkey ball")}
		for _, payload := range input {
			data = core.AppendBytesWithLength(data, payload)
		}

		var payload []byte
		for _, expected := range input {
			var err error
			data, err = core.ConsumeBytesWithLength(data, &payload)
			So(err, ShouldBeNil)
			So(string(payload), ShouldEqual, string(expected))

		}

		Convey("All of the data should have been consumed", func() {
			So(len(data), ShouldEqual, 0)
		})

		Convey("Improperly encoded bytes will returns errors", func() {
			// This test assumes that strings are encoded with 16 bits of length as a prefix.
			var data []byte
			data = core.AppendUint16(data, 10000)
			var payload []byte
			_, err := core.ConsumeBytesWithLength(data, &payload)
			So(err, ShouldNotBeNil)
		})
	})

}

func BenchmarkEncodingUint32sOnEmptySlice(b *testing.B) {
	var data []byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data = core.AppendUint32(data, 0)
	}
}

func BenchmarkEncoding1000000Uint32s(b *testing.B) {
	data := make([]byte, 4*1000000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data = data[0:0]
		for j := 0; j < 1000000; j++ {
			data = core.AppendUint32(data, 0)
		}
	}
}

func BenchmarkConsumingUint32s(b *testing.B) {
	data := make([]byte, b.N*4)
	b.ResetTimer()
	var payload uint32
	for i := 0; i < b.N; i++ {
		data = core.ConsumeUint32(data, &payload)
	}
}
