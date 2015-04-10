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
	// SourceAddr is set, for incoming dispatched, to the addr of the host that sent it to us.
	SourceAddr network.Addr

	// TODO: If we make the configs available when serializing/parsing we could remove the bytes
	// needed for the Target field if it is a broadcast stream.
	Target NodeId
	Source NodeId

	Stream      StreamId
	Sequence    SequenceId
	Subsequence SubsequenceIndex

	// Data holds all of the user-level data.
	Data []byte
}

// SerializedLength returns the number of bytes needed to serialize chunk.
func (c *Chunk) SerializedLength() int {
	return 14 + len(c.Data)
}

// SequenceStart returns the SequenceId of the first chunk in the packet that this chunk originated
// from.
func (c *Chunk) SequenceStart() SequenceId {
	if c.Subsequence == 0 {
		return c.Sequence
	}
	// Subsequence indexes are 1-based, so we have to add a 1 here to make up for the 1 that we are
	// subtracting with the -c.Subsequence.
	return 1 + c.Sequence - SequenceId(c.Subsequence)
}

// AppendChunk serializes payload, appends it to buf, and returns buf.
func AppendChunk(buf []byte, payload *Chunk) []byte {
	buf = AppendNodeId(buf, payload.Source)
	buf = AppendNodeId(buf, payload.Target)
	buf = AppendStreamId(buf, payload.Stream)
	buf = AppendSequenceId(buf, payload.Sequence)
	buf = AppendSubsequenceIndex(buf, payload.Subsequence)
	return AppendBytesWithLength(buf, payload.Data)
}

// ConsumeChunk consumes a single chunk off the front of buf, returning buf or an error.
func ConsumeChunk(buf []byte, payload *Chunk) ([]byte, error) {
	buf = ConsumeNodeId(buf, &payload.Source)
	buf = ConsumeNodeId(buf, &payload.Target)
	buf = ConsumeStreamId(buf, &payload.Stream)
	buf = ConsumeSequenceId(buf, &payload.Sequence)
	buf = ConsumeSubsequenceIndex(buf, &payload.Subsequence)
	return ConsumeBytesWithLength(buf, &payload.Data)
}

// ParseChunks parses buf, which should be data serialized by BatchAndSend, and returns the
// resulting chunks.
func ParseChunks(buf []byte) ([]Chunk, error) {
	var crc uint32
	buf = ConsumeUint32(buf, &crc)
	if crc != crc32.Checksum(buf, crcTable) {
		return nil, fmt.Errorf("CRC mismatch")
	}
	var chunks []Chunk
	for len(buf) > 0 {
		var chunk Chunk
		var err error
		buf, err = ConsumeChunk(buf, &chunk)
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

// sendSerializedData sets the leading CRC on buf and writes the data to conn.  Any errors on the
// write will be logged but otherwise ignored.
func sendSerializedData(buf []byte, conn io.Writer) {
	// Prefix buf with a crc of everything we've appended to it.
	AppendUint32(buf[0:0], crc32.Checksum(buf[4:], crcTable))
	_, err := conn.Write(buf)
	if err != nil {
		log.Printf("Failed to write %d bytes in BatchAndSend: %v", err)
	}
}

// BatchAndSend reads from chunks and serialiezes them and sends them along conn.  It will batch
// together multiple chunks into a single send, and it chooses a cutoff based on cutoffBytes and
// cutoffMs.  If either cutoffBytes or cutoffMs is less than or equal to zero, BatchAndSend will
// send each chunk individually.
func BatchAndSend(chunks <-chan Chunk, conn io.Writer, c clock.Clock, cutoffBytes int, cutoffMs int) {
	if cutoffMs < 0 {
		cutoffMs = 0
	}
	var timeout <-chan time.Time
	buf := AppendUint32(nil, 0) // Make room for a CRC.
	numChunks := 0
	for {
		select {
		case chunk, ok := <-chunks:
			if !ok {
				// Send any queued up chunks before quitting.
				sendSerializedData(buf, conn)
				return
			}
			chunkLength := chunk.SerializedLength()
			if len(buf)+chunkLength >= cutoffBytes && numChunks > 0 {
				sendSerializedData(buf, conn)
				numChunks = 0
				buf = buf[0:4] // Leave 4 bytes at the front for the CRC
				timeout = nil
			}
			buf = AppendChunk(buf, &chunk)
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

func ReceiveAndSplit(conn ReadFromer, chunks chan<- Chunk, maxChunkSize int) {
	defer close(chunks)
	buf := make([]byte, maxChunkSize)
	for {
		n, addr, err := conn.ReadFrom(buf)
		if err != nil {
			log.Printf("ReceiveAndSplit connection was closed.")
			return
		}
		parsedChunks, err := ParseChunks(buf[0:n])
		if err != nil {
			log.Printf("Error parsing chunks: %v", err)
			continue
		}
		go func() {
			for _, chunk := range parsedChunks {
				chunk.SourceAddr = addr
				chunks <- chunk
			}
		}()
	}
}
