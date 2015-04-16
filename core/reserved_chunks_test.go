package core_test

import (
	"testing"

	"github.com/runningwild/sluice/core"
	. "github.com/smartystreets/goconvey/convey"
)

func mergeResendData(dst, src core.ResendRequest) {
	for stream, sequences := range src {
		for _, sequence := range sequences {
			dst[stream] = append(dst[stream], sequence)
		}
	}
}

func TestResendChunks(t *testing.T) {
	req := core.ResendRequest{
		10:   []core.SequenceId{5, 6, 7, 8, 9},
		100:  []core.SequenceId{1},
		2500: []core.SequenceId{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18},
	}
	Convey("The data that comes out of a resend chunk is the same as the data that went into it.", t, func() {
		var config core.Config
		config.MaxChunkDataSize = 10000
		datas := core.MakeResendChunkDatas(&config, req)
		So(len(datas), ShouldEqual, 1)
		parsed, err := core.ParseResendChunkData(datas[0])
		So(err, ShouldBeNil)
		So(parsed, ShouldResemble, req)
	})
	Convey("Resend data can be split across multiple chunks.", t, func() {
		var config core.Config
		config.MaxChunkDataSize = 20
		datas := core.MakeResendChunkDatas(&config, req)
		So(len(datas), ShouldBeGreaterThan, 1)
		merged := make(core.ResendRequest)
		for _, data := range datas {
			parsed, err := core.ParseResendChunkData(data)
			So(err, ShouldBeNil)
			mergeResendData(merged, parsed)
		}
		So(merged, ShouldResemble, req)
	})
	Convey("Malformed resend chunks return errors.", t, func() {
		_, err := core.ParseResendChunkData([]byte{1})
		So(err, ShouldNotBeNil)
	})
}

func TestTruncateChunks(t *testing.T) {
	req := core.TruncateRequest{}
	for i := 1; i < 100; i++ {
		req[core.StreamId(i)] = core.SequenceId(i + 1)
	}
	Convey("The data that comes out of a truncate chunk is the same as the data that went into it.", t, func() {
		var config core.Config
		config.MaxChunkDataSize = 10000
		datas := core.MakeTruncateChunkDatas(&config, req)
		So(len(datas), ShouldEqual, 1)
		parsed, err := core.ParseTruncateChunkData(datas[0])
		So(err, ShouldBeNil)
		So(parsed, ShouldResemble, req)
	})
	Convey("Truncate data can be split across multiple chunks.", t, func() {
		var config core.Config
		config.MaxChunkDataSize = 20
		datas := core.MakeTruncateChunkDatas(&config, req)
		So(len(datas), ShouldBeGreaterThan, 1)
		merged := make(core.TruncateRequest)
		for _, data := range datas {
			parsed, err := core.ParseTruncateChunkData(data)
			So(err, ShouldBeNil)
			for stream, sequence := range parsed {
				_, ok := merged[stream]
				So(ok, ShouldBeFalse)
				merged[stream] = sequence
			}
		}
		So(merged, ShouldResemble, req)
	})
	Convey("Malformed truncate chunks return errors.", t, func() {
		_, err := core.ParseTruncateChunkData([]byte{1})
		So(err, ShouldNotBeNil)
	})
}

func TestPositionChunks(t *testing.T) {
	req := core.PositionUpdate{}
	for i := 1; i < 100; i++ {
		req[core.StreamId(i)] = core.SequenceId(i + 1)
	}
	Convey("The data that comes out of a position chunk is the same as the data that went into it.", t, func() {
		var config core.Config
		config.MaxChunkDataSize = 10000
		datas := core.MakePositionChunkDatas(&config, req)
		So(len(datas), ShouldEqual, 1)
		parsed, err := core.ParsePositionChunkData(datas[0])
		So(err, ShouldBeNil)
		So(parsed, ShouldResemble, req)
	})
	Convey("Position data can be split across multiple chunks.", t, func() {
		var config core.Config
		config.MaxChunkDataSize = 20
		datas := core.MakePositionChunkDatas(&config, req)
		So(len(datas), ShouldBeGreaterThan, 1)
		merged := make(core.PositionUpdate)
		for _, data := range datas {
			parsed, err := core.ParsePositionChunkData(data)
			So(err, ShouldBeNil)
			for stream, sequence := range parsed {
				_, ok := merged[stream]
				So(ok, ShouldBeFalse)
				merged[stream] = sequence
			}
		}
		So(merged, ShouldResemble, req)
	})
	Convey("Malformed position chunks return errors.", t, func() {
		_, err := core.ParsePositionChunkData([]byte{1})
		So(err, ShouldNotBeNil)
	})
}
