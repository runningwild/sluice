package core

import (
	"fmt"
)

func ConsumeBool(data []byte, payload *bool) []byte {
	*payload = data[0] != 0
	return data[1:]
}

func ConsumeUint32(data []byte, payload *uint32) []byte {
	*payload = (uint32(data[3]) << 24) | (uint32(data[2]) << 16) | (uint32(data[1]) << 8) | uint32(data[0])
	return data[4:]
}

func ConsumeUint16(data []byte, payload *uint16) []byte {
	*payload = (uint16(data[1]) << 8) | uint16(data[0])
	return data[2:]
}

func ConsumeStreamId(data []byte, payload *StreamId) []byte {
	var p uint16
	data = ConsumeUint16(data, &p)
	*payload = StreamId(p)
	return data
}

func ConsumeNodeId(data []byte, payload *NodeId) []byte {
	var p uint16
	data = ConsumeUint16(data, &p)
	*payload = NodeId(p)
	return data
}

func ConsumeSequenceId(data []byte, payload *SequenceId) []byte {
	var p uint32
	data = ConsumeUint32(data, &p)
	*payload = SequenceId(p)
	return data
}

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

func AppendBool(data []byte, payload bool) []byte {
	if payload {
		return append(data, 1)
	}
	return append(data, 0)
}

func AppendUint32(data []byte, payload uint32) []byte {
	data = append(data, byte(payload&0xff))
	payload = payload >> 8
	data = append(data, byte(payload&0xff))
	payload = payload >> 8
	data = append(data, byte(payload&0xff))
	payload = payload >> 8
	return append(data, byte(payload&0xff))
}

func AppendUint16(data []byte, payload uint16) []byte {
	data = append(data, byte(payload&0xff))
	payload = payload >> 8
	return append(data, byte(payload&0xff))
}

func AppendStreamId(data []byte, payload StreamId) []byte {
	return AppendUint16(data, uint16(payload))
}

func AppendNodeId(data []byte, payload NodeId) []byte {
	return AppendUint16(data, uint16(payload))
}

func AppendSequenceId(data []byte, payload SequenceId) []byte {
	return AppendUint32(data, uint32(payload))
}

func AppendStringWithLength(data []byte, payload string) []byte {
	data = AppendUint16(data, uint16(len(payload)))
	for i := 0; i < len(payload); i++ {
		data = append(data, payload[i])
	}
	return data
}

func AppendBytesWithLength(data []byte, payload []byte) []byte {
	data = AppendUint16(data, uint16(len(payload)))
	for i := 0; i < len(payload); i++ {
		data = append(data, payload[i])
	}
	return data
}
