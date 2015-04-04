package core

import (
	"fmt"
	// "github.com/runningwild/clock"
	"github.com/runningwild/network"

	"hash/crc32"
	// "io"
	// "time"
)

var crcTable *crc32.Table

func init() {
	crcTable = crc32.MakeTable(crc32.Castagnoli)
}

type Packet struct {
	// SourceAddr is set, on received packets, to the addr of the host that sent it to us.
	SourceAddr network.Addr

	Source    NodeId
	Target    NodeId
	Stream    StreamId
	Sequenced bool
	Sequence  SequenceId // Only set if Sequenced is true

	// Data is the raw data, all user-level data is sent through this field.
	Data []byte
}

// AppendPacket serializes packet, appends it to buf, and returns buf.
func AppendPacket(packet *Packet, buf []byte) []byte {
	buf = AppendNodeId(buf, packet.Source)
	buf = AppendNodeId(buf, packet.Target)
	buf = AppendStreamId(buf, packet.Stream)
	buf = AppendBool(buf, packet.Sequenced)
	if packet.Sequenced {
		buf = AppendSequenceId(buf, packet.Sequence)
	}
	return AppendBytesWithLength(buf, packet.Data)
}

// SerializeRawPackets truncates buf, serializes each packet in packets, appends them to buf,
// prefixes buf with a crc, and returns buf.
func SerializePackets(packets []Packet, buf []byte) []byte {
	buf = buf[0:0]
	buf = AppendUint32(buf, 0) // Make room for a CRC later
	for _, packet := range packets {
		buf = AppendNodeId(buf, packet.Source)
		buf = AppendNodeId(buf, packet.Target)
		buf = AppendStreamId(buf, packet.Stream)
		buf = AppendBool(buf, packet.Sequenced)
		if packet.Sequenced {
			buf = AppendSequenceId(buf, packet.Sequence)
		}
		buf = AppendBytesWithLength(buf, packet.Data)
	}

	// This is a little wonky, but it actually jams the crc at the front.
	AppendUint32(buf[0:0], crc32.Checksum(buf[4:], crcTable))
	return buf
}

// ConsumePacket consumes a single packet off the front of buf, returning buf or an error.
func ConsumePacket(buf []byte, payload *Packet) ([]byte, error) {
	buf = ConsumeNodeId(buf, &payload.Source)
	buf = ConsumeNodeId(buf, &payload.Target)
	buf = ConsumeStreamId(buf, &payload.Stream)
	buf = ConsumeBool(buf, &payload.Sequenced)
	if payload.Sequenced {
		buf = ConsumeSequenceId(buf, &payload.Sequence)
	} else {
		payload.Sequence = 0
	}
	return ConsumeBytesWithLength(buf, &payload.Data)
}

// ParseRawPackets parses buf, which was data serialized by SerializeRawPackets,
// and puts those packets into raws.  Returns true iff the crc test passes.
func ParsePackets(buf []byte) ([]Packet, error) {
	var crc uint32
	buf = ConsumeUint32(buf, &crc)
	if crc != crc32.Checksum(buf, crcTable) {
		return nil, fmt.Errorf("CRC mismatch")
	}
	var packets []Packet
	for len(buf) > 0 {
		var packet Packet
		var err error
		buf, err = ConsumePacket(buf, &packet)
		if err != nil {
			return nil, err
		}
		packets = append(packets, packet)
	}
	return packets, nil
}

// // batchDoSend makes sure that there are packets to send, and if so, serializes
// // them and sends them through conn.
// func batchDoSend(conn io.Writer, raws []RawPacket, buf *[]byte) {
// 	if len(raws) == 0 {
// 		// Don't send a packet with nothing in it.
// 		return
// 	}
// 	length := SerializeRawPackets(raws, buf)
// 	conn.Write((*buf)[:length]) // TODO: check the error and log it
// 	// fmt.Printf("Sent packet: %v\n", (*buf)[:length])
// }

// // BatchAndSend collects RawPackets from the channel raw, batches the data
// // into a single packet, adds a crc, and sends it through conn.  cutoffMs
// // indicates the maximum amount of time to wait before sending a packet, and
// // cutoffBytes indicates how large a packet needs to be to send it before it
// // will be sent before cutoffMs have passed.
// func BatchAndSend(rawOut <-chan RawPacket, conn io.Writer, c clock.Clock, cutoffBytes int, cutoffMs int) {
// 	var ticker <-chan time.Time
// 	var buf []byte
// 	var raws []RawPacket
// 	for {
// 		select {
// 		case raw := <-rawOut:
// 			raws = append(raws, raw)
// 			if serializedLength(raws) > cutoffBytes {
// 				if len(raws) == 1 {
// 					// Even if this one packet is too large there is really nothing
// 					// else to do but try and send it anyway.
// 					batchDoSend(conn, raws, &buf)
// 					raws = raws[0:0]
// 				} else {
// 					// Don't send the packet that pushed it over the edge because
// 					// the resulting packet might be too large.
// 					batchDoSend(conn, raws[0:len(raws)-1], &buf)
// 					raws = raws[len(raws)-1:]
// 				}
// 				ticker = nil
// 			}
// 			if len(raws) > 0 && ticker == nil {
// 				// We don't want to set this ticker again if it's already set,
// 				// otherwise a constant stream of packets could actually starve
// 				// the conncetion.
// 				ticker = c.After(time.Duration(cutoffMs) * time.Millisecond)
// 			}

// 		case <-ticker:
// 			// ticker will be nil unless a packet has arrived recently, so this
// 			// section won't we run if there are no packets available to send.
// 			batchDoSend(conn, raws, &buf)
// 			raws = raws[0:0]
// 			ticker = nil
// 		}
// 	}
// }

// type ReadFromer interface {
// 	ReadFrom(buf []byte) (n int, addr network.Addr, err error)
// }

// // ReceiveAndSplit takes packets that were batched together by BatchAndSend,
// // breaks them up into individual packets, and then sends each along rawIn.
// func ReceiveAndSplit(conn ReadFromer, rawIn chan<- RawPacket, MaxPacketSize int) {
// 	buf := make([]byte, MaxPacketSize)
// 	var raws []RawPacket
// 	for {
// 		n, addr, err := conn.ReadFrom(buf)
// 		if err != nil {
// 			// fmt.Printf("Error reading from %v\n", conn)
// 			// TODO: Maybe log this?
// 			close(rawIn)
// 			return
// 		}
// 		if ParseRawPackets(buf[0:n], &raws) {
// 			for _, raw := range raws {
// 				raw.SourceAddr = addr
// 				rawIn <- raw
// 			}
// 			raws = raws[0:0]
// 		} else {
// 			// fmt.Printf("FAILED TO PARSE PACKETS\n")
// 		}
// 	}
// }
