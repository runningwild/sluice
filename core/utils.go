package core

import (
	"fmt"
)

// ConsumeBool consumes a boolean payload from the front of data and returns data.
func ConsumeBool(data []byte, payload *bool) []byte {
	*payload = data[0] != 0
	return data[1:]
}

// ConsumeUint32 consumes a uint32 payload from the front of data and returns data.
func ConsumeUint32(data []byte, payload *uint32) []byte {
	*payload = (uint32(data[3]) << 24) | (uint32(data[2]) << 16) | (uint32(data[1]) << 8) | uint32(data[0])
	return data[4:]
}

// ConsumeUint16 consumes a uint16 payload from the front of data and returns data.
func ConsumeUint16(data []byte, payload *uint16) []byte {
	*payload = (uint16(data[1]) << 8) | uint16(data[0])
	return data[2:]
}

// ConsumeUint8 consumes a uint8 payload from the front of data and returns data.
func ConsumeUint8(data []byte, payload *uint8) []byte {
	*payload = (uint8(data[1]) << 8) | uint8(data[0])
	return data[1:]
}

// ConsumeStreamId consumes a StreamId payload from the front of data and returns data.
func ConsumeStreamId(data []byte, payload *StreamId) []byte {
	var p uint16
	data = ConsumeUint16(data, &p)
	*payload = StreamId(p)
	return data
}

// ConsumeNodeId consumes a NodeId payload from the front of data and returns data.
func ConsumeNodeId(data []byte, payload *NodeId) []byte {
	var p uint16
	data = ConsumeUint16(data, &p)
	*payload = NodeId(p)
	return data
}

// ConsumeSequenceId consumes a SequenceId payload from the front of data and returns data.
func ConsumeSequenceId(data []byte, payload *SequenceId) []byte {
	var p uint32
	data = ConsumeUint32(data, &p)
	*payload = SequenceId(p)
	return data
}

// ConsumeSubsequenceIndex consumes a SubsequenceIndex payload from the front of data and returns data.
func ConsumeSubsequenceIndex(data []byte, payload *SubsequenceIndex) []byte {
	var p uint16
	data = ConsumeUint16(data, &p)
	*payload = SubsequenceIndex(p)
	return data
}

// ConsumeStringWithLength consumes a string payload from the front of data and returns data.
func ConsumeStringWithLength(data []byte, payload *string) ([]byte, error) {
	var length uint16
	data = ConsumeUint16(data, &length)
	if int(length) > len(data) {
		// TODO: Might want to log this error
		return data, fmt.Errorf("Tried to consume a longer string than is available (%d vs %d)", length, len(data))
	}
	*payload = string(data[0:int(length)])
	return data[int(length):], nil
}

// ConsumeBytesWithLength consumes a bytes payload from the front of data and returns data.
func ConsumeBytesWithLength(data []byte, payload *[]byte) ([]byte, error) {
	var length uint16
	data = ConsumeUint16(data, &length)
	if int(length) > len(data) {
		// TODO: Might want to log this error
		return data, fmt.Errorf("Tried to consume a longer string than is available (%d vs %d)", length, len(data))
	}
	*payload = make([]byte, int(length))
	copy(*payload, data[0:int(length)])
	return data[int(length):], nil
}

// AppendBool appends a boolean payload to data and returns data.
func AppendBool(data []byte, payload bool) []byte {
	if payload {
		return append(data, 1)
	}
	return append(data, 0)
}

// AppendUint32 appends a uint32 payload to data and returns data.
func AppendUint32(data []byte, payload uint32) []byte {
	data = append(data, byte(payload&0xff))
	payload = payload >> 8
	data = append(data, byte(payload&0xff))
	payload = payload >> 8
	data = append(data, byte(payload&0xff))
	payload = payload >> 8
	return append(data, byte(payload&0xff))
}

// AppendUint16 appends a uint16 payload to data and returns data.
func AppendUint16(data []byte, payload uint16) []byte {
	data = append(data, byte(payload&0xff))
	payload = payload >> 8
	return append(data, byte(payload&0xff))
}

// AppendUint8 appends a uint8 payload to data and returns data.
func AppendUint8(data []byte, payload uint8) []byte {
	return append(data, payload)
}

// AppendStreamId appends a StreamId payload to data and returns data.
func AppendStreamId(data []byte, payload StreamId) []byte {
	return AppendUint16(data, uint16(payload))
}

// AppendNodeId appends a NodeId payload to data and returns data.
func AppendNodeId(data []byte, payload NodeId) []byte {
	return AppendUint16(data, uint16(payload))
}

// AppendSequenceId appends a SequenceId payload to data and returns data.
func AppendSequenceId(data []byte, payload SequenceId) []byte {
	return AppendUint32(data, uint32(payload))
}

// AppendSubsequenceIndex appends a SubsequenceIndex payload to data and returns data.
func AppendSubsequenceIndex(data []byte, payload SubsequenceIndex) []byte {
	return AppendUint16(data, uint16(payload))
}

// AppendStringWithLength appends a string payload to data and returns data.
func AppendStringWithLength(data []byte, payload string) []byte {
	data = AppendUint16(data, uint16(len(payload)))
	for i := 0; i < len(payload); i++ {
		data = append(data, payload[i])
	}
	return data
}

// AppendBytesWithLength appends a bytes payload to data and returns data.
func AppendBytesWithLength(data []byte, payload []byte) []byte {
	data = AppendUint16(data, uint16(len(payload)))
	for i := 0; i < len(payload); i++ {
		data = append(data, payload[i])
	}
	return data
}
