package core

import (
	"fmt"
	"github.com/runningwild/clock"
	"github.com/runningwild/network"
	"log"

	"hash/crc32"
	"io"
	"time"
)

var crcTable *crc32.Table

func init() {
	crcTable = crc32.MakeTable(crc32.Castagnoli)
}

type Chunk struct {
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

// AppendChunk serializes packet, appends it to buf, and returns buf.
func AppendChunk(buf []byte, packet *Chunk) []byte {
	buf = AppendNodeId(buf, packet.Source)
	buf = AppendNodeId(buf, packet.Target)
	buf = AppendStreamId(buf, packet.Stream)
	buf = AppendBool(buf, packet.Sequenced)
	if packet.Sequenced {
		buf = AppendSequenceId(buf, packet.Sequence)
	}
	return AppendBytesWithLength(buf, packet.Data)
}

// serializedLength returns the number of bytes needed to serialize packet.
func serializedLength(packet *Chunk) int {
	total := 7
	if packet.Sequenced {
		total += 4
	}
	return total + 4 + len(packet.Data)
}

// ConsumeChunk consumes a single packet off the front of buf, returning buf or an error.
func ConsumeChunk(buf []byte, payload *Chunk) ([]byte, error) {
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

// ParseRawChunks parses buf, which was data serialized by SerializeRawChunks,
// and puts those packets into raws.  Returns true iff the crc test passes.
func ParseChunks(buf []byte) ([]Chunk, error) {
	var crc uint32
	buf = ConsumeUint32(buf, &crc)
	if crc != crc32.Checksum(buf, crcTable) {
		return nil, fmt.Errorf("CRC mismatch")
	}
	var packets []Chunk
	for len(buf) > 0 {
		var packet Chunk
		var err error
		buf, err = ConsumeChunk(buf, &packet)
		if err != nil {
			return nil, err
		}
		packets = append(packets, packet)
	}
	return packets, nil
}

func sendSerializedData(buf []byte, conn io.Writer) {
	// Prefix buf with a crc of everything we've appended to it.
	AppendUint32(buf[0:0], crc32.Checksum(buf[4:], crcTable))
	fmt.Printf("Writing %v\n", buf)
	_, err := conn.Write(buf)
	if err != nil {
		log.Printf("Failed to write %d bytes in BatchAndSend: %v", err)
	}
}

// BatchAndSend reads from packets and serialiezes them and sends them along conn.  It will batch
// together multiple packets into a single send, and it chooses a cutoff based on cutoffBytes and
// cutoffMs.  If either cutoffBytes or cutoffMs is less than or equal to zero, BatchAndSend will
// send each packet individually.
func BatchAndSend(packets <-chan Chunk, conn io.Writer, c clock.Clock, cutoffBytes int, cutoffMs int) {
	if cutoffMs < 0 {
		cutoffMs = 0
	}
	var timeout <-chan time.Time
	buf := AppendUint32(nil, 0) // Make room for a CRC.
	numChunks := 0
	for {
		select {
		case packet, ok := <-packets:
			if !ok {
				// Send any queued up packets before quitting.
				sendSerializedData(buf, conn)
				return
			}
			packetLength := serializedLength(&packet)
			if len(buf)+packetLength >= cutoffBytes && numChunks > 0 {
				sendSerializedData(buf, conn)
				numChunks = 0
				buf = buf[0:4] // Leave 4 bytes at the front for the CRC
				timeout = nil
			}
			buf = AppendChunk(buf, &packet)
			numChunks++
			if timeout == nil {
				timeout = c.After(time.Millisecond * time.Duration(cutoffMs))
			}

		case <-timeout:
			sendSerializedData(buf, conn)
			numChunks = 0
			buf = buf[0:4] // Leave 4 bytes at the front for the CRC
			timeout = nil

		}
	}
}

type ReadFromer interface {
	ReadFrom(buf []byte) (n int, addr network.Addr, err error)
}

func ReceiveAndSplit(conn ReadFromer, packets chan<- Chunk, maxChunkSize int) {
	defer close(packets)
	buf := make([]byte, maxChunkSize)
	for {
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			log.Printf("ReceiveAndSplit connection was closed.")
			return
		}
		parsedChunks, err := ParseChunks(buf[0:n])
		if err != nil {
			log.Printf("Error parsing packets: %v", err)
			continue
		}
		go func() {
			for _, packet := range parsedChunks {
				packet.SourceAddr = addr
				packets <- packet
			}
		}()
	}
}
