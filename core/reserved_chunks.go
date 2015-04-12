package core

import (
	"fmt"
)

// ResendRequest is a map from StreamId to a list of SequenceIds of chunks on that stream that need
// to be resent to the host.
type ResendRequest map[StreamId][]SequenceId

// MakeResendChunkDatas serializes the data in req into zero or more chunks that convey all of the
// data expressed in it.  Each data can be sent as an independant chunk that will be usable even if
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

// ParseResendChunkData parses a resend chunk data into a ResendRequest.
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

// ResendRequest is a map from StreamId to a list of SequenceIds of chunks on that stream that need
// to be resent to the host.
type TruncateRequest map[StreamId]SequenceId

// MakeTruncateChunkDatas serializes the data in req into zero or more chunks that convey all of the
// data expressed in it.  Each data can be sent as an independant chunk that will be usable even if
// none of the other chunks are received.  An individual req chunk's data is repeated pairs of
// <StreamId, SequenceId>
func MakeTruncateChunkDatas(config *Config, req TruncateRequest) [][]byte {
	var ret [][]byte
	var current []byte

	for stream, sequence := range req {
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

// ParseTruncateChunkData parses a resend chunk data into a TruncateRequest.
func ParseTruncateChunkData(data []byte) (req TruncateRequest, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("unexpected parse error while parsing a resend chunk: %q", r)
		}
	}()
	req = make(TruncateRequest)
	for len(data) > 0 {
		var stream StreamId
		data = ConsumeStreamId(data, &stream)
		var sequence SequenceId
		data = ConsumeSequenceId(data, &sequence)
		req[stream] = sequence
	}
	return req, err
}
