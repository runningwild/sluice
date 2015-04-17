package core

import (
	"fmt"
)

// ResendRequest is a map from StreamId to a list of SequenceIds of chunks on that stream that need
// to be resent to the host.
type ResendRequest map[StreamId][]SequenceId

// MakeResendChunkDatas serializes the data in req into zero or more chunks that convey all of the
// data expressed in it.  Each chunk can be sent as an independant chunk that will be usable even if
// none of the other chunks are received.  An individual req chunk's data is repeated pairs of
// <StreamId, SequenceId>
func MakeResendChunkDatas(config *Config, req ResendRequest) [][]byte {
	var ret [][]byte
	var current []byte

	for stream, sequences := range req {
		for _, sequence := range sequences {
			if len(current) >= config.MaxChunkDataSize-4 {
				ret = append(ret, current)
				current = nil
			}
			current = AppendStreamId(current, stream)
			current = AppendSequenceId(current, sequence)
		}
	}

	if len(current) > 0 {
		ret = append(ret, current)
	}
	return ret
}

// ParseResendChunkData parses resend chunk data into a ResendRequest.
func ParseResendChunkData(data []byte) (req ResendRequest, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unexpected parse error while parsing a resend chunk: %q", r)
		}
	}()
	req = make(ResendRequest)
	for len(data) > 0 {
		var stream StreamId
		data = ConsumeStreamId(data, &stream)
		var sequence SequenceId
		data = ConsumeSequenceId(data, &sequence)
		req[stream] = append(req[stream], sequence)
	}
	return req, err
}

// streamIdToSequenceId is a generic chunk structure that is used by multiple chunks.
type streamIdToSequenceId map[StreamId]SequenceId

func (s streamIdToSequenceId) makeChunkDatas(config *Config) [][]byte {
	var ret [][]byte
	var current []byte

	for stream, sequence := range s {
		if len(current) >= config.MaxChunkDataSize-4 {
			ret = append(ret, current)
			current = nil
		}
		current = AppendStreamId(current, stream)
		current = AppendSequenceId(current, sequence)
	}

	if len(current) > 0 {
		ret = append(ret, current)
	}
	return ret
}

func (s *streamIdToSequenceId) parseChunkDatas(data []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unexpected parse error while parsing a resend chunk: %q", r)
		}
	}()
	for len(data) > 0 {
		var stream StreamId
		data = ConsumeStreamId(data, &stream)
		var sequence SequenceId
		data = ConsumeSequenceId(data, &sequence)
		(*s)[stream] = sequence
	}
	return err
}

// TruncateRequest is a mapping from StreamId to the newest SequenceId that the client can truncate.
type TruncateRequest streamIdToSequenceId

// MakeTruncateChunkDatas serializes the data in req into zero or more chunks that convey all of the
// data expressed in it.  Each chunk can be sent as an independant chunk that will be usable even if
// none of the other chunks are received.  An individual req chunk's data is repeated pairs of
// <StreamId, SequenceId>
func MakeTruncateChunkDatas(config *Config, req TruncateRequest) [][]byte {
	return streamIdToSequenceId(req).makeChunkDatas(config)
}

// ParseTruncateChunkData parses truncate chunk data into a TruncateRequest.
func ParseTruncateChunkData(data []byte) (TruncateRequest, error) {
	s := make(streamIdToSequenceId)
	err := s.parseChunkDatas(data)
	if err != nil {
		return nil, err
	}
	return TruncateRequest(s), nil
}

// PositionUpdate is a mapping from StreamId to the newest SequenceId that the client has sent.
type PositionUpdate streamIdToSequenceId

// MakePositionChunkDatas serializes the data in req into zero or more chunks that convey all of the
// data expressed in it.  Each chunk can be sent as an independant chunk that will be usable even if
// none of the other chunks are received.  An individual req chunk's data is repeated pairs of
// <StreamId, SequenceId>
func MakePositionChunkDatas(config *Config, req PositionUpdate) [][]byte {
	return streamIdToSequenceId(req).makeChunkDatas(config)
}

// ParsePositionChunkData parses position chunk data into a PositionUpdate.
func ParsePositionChunkData(data []byte) (PositionUpdate, error) {
	s := make(streamIdToSequenceId)
	err := s.parseChunkDatas(data)
	if err != nil {
		return nil, err
	}
	return PositionUpdate(s), nil
}
