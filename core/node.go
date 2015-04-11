package core

import (
	"fmt"
)

type Node struct{}

// WriterRoutine reads channel packets, converts each packet into one or more chunks each, then
// sends those along channel chunks.
func WriterRoutine(config StreamConfig, target NodeId, maxChunkDataSize int, packets <-chan []byte, chunks chan<- Chunk) {
	if maxChunkDataSize <= 0 {
		panic("maxChunkDataSize must be positive.")
	}
	if config.Broadcast && target != 0 {
		panic("Cannot target with a broadcast stream.")
	}
	var sequence SequenceId = 0
	for packet := range packets {
		if len(packet) <= maxChunkDataSize {
			chunks <- Chunk{
				Source:      0, // Irrelevant unless being sent from the host
				Target:      target,
				Stream:      config.Id,
				Sequence:    sequence,
				Subsequence: 0,
				Data:        packet,
			}
			sequence++
			continue
		}

		// This will break packet into chunks such that len(chunk.Data) <= maxChunkDataSize.
		// A zero-length chunk will be appended if the last chunk length would otherwise have been
		// exactly maxChunkDataSize.  This way the last chunk in a sequence can be detected by
		// checking its size.
		var index SubsequenceIndex = 1
		lastSend := maxChunkDataSize
		for len(packet) > 0 || lastSend == maxChunkDataSize {
			chunkData := packet
			if len(chunkData) > maxChunkDataSize {
				chunkData = chunkData[0:maxChunkDataSize]
			}
			chunks <- Chunk{
				Source:      0, // Irrelevant unless being sent from the host
				Target:      target,
				Stream:      config.Id,
				Sequence:    sequence,
				Subsequence: index,
				Data:        chunkData,
			}
			index++
			sequence++
			packet = packet[len(chunkData):]
			lastSend = len(chunkData)
		}
	}
}

// chunkSequencer tracks all chunks that came from the same packet.
type chunkSequencer struct {
	chunks    map[SubsequenceIndex]*Chunk
	lastChunk *Chunk
	numChunks int

	// sequence is the SequenceId of the first chunk in the packet.
	sequence SequenceId
}

func makeChunkSequencer(sequence SequenceId) *chunkSequencer {
	return &chunkSequencer{
		chunks:   make(map[SubsequenceIndex]*Chunk),
		sequence: sequence,
	}
}

func (cs *chunkSequencer) AddChunk(chunk *Chunk) {
	if chunk.SequenceStart() != cs.sequence {
		panic(fmt.Sprintf("Chunk was from sequence %d, expected %d.", chunk.SequenceStart(), cs.sequence))
	}
	cs.chunks[chunk.Subsequence] = chunk
	if cs.numChunks == 0 {
		if chunk.Subsequence == 0 {
			cs.numChunks = 1
		} else if cs.lastChunk != nil && len(chunk.Data) != len(cs.lastChunk.Data) {
			if len(chunk.Data) < len(cs.lastChunk.Data) {
				cs.numChunks = int(chunk.Subsequence)
			} else {
				cs.numChunks = int(cs.lastChunk.Subsequence)
			}
		} else {
			cs.lastChunk = chunk
		}
	}
}

func (cs *chunkSequencer) Done() bool {
	// Hopefully cs.numChunks < len(cs.chunks) will never happen, but if a malicious client was
	// sending us data it could, and we can't just panic because of that.
	return cs.numChunks > 0 && cs.numChunks <= len(cs.chunks)
}

func (cs *chunkSequencer) GetPacket() []byte {
	// First handle the simplest case of a packet that didn't get split into multiple chunks.
	if cs.numChunks == 1 {
		return cs.chunks[0].Data
	}

	var packet []byte
	// Remember that subsequence indexes are 1-indexed.
	for i := 1; i <= cs.numChunks; i++ {
		for _, b := range cs.chunks[SubsequenceIndex(i)].Data {
			packet = append(packet, b)
		}
	}
	return packet
}

// ChunkMerger provides a way of tracking incoming chunks and assembling them into packets.
type ChunkMerger interface {
	// AddChunk adds chunk to the merger.  Any packets that are available after this addition are
	// returned in the order they were sent.
	AddChunk(chunk Chunk) [][]byte
}

type UnreliableUnorderedChunkMerger struct {
	chunks  map[SequenceId]*chunkSequencer
	horizon SequenceId
	maxAge  SequenceId
}

func MakeUnreliableUnorderedChunkMerger(maxAge SequenceId) ChunkMerger {
	if maxAge < 0 {
		maxAge = 0
	}
	return &UnreliableUnorderedChunkMerger{
		chunks:  make(map[SequenceId]*chunkSequencer),
		horizon: 0,
		maxAge:  maxAge,
	}
}
func (ct *UnreliableUnorderedChunkMerger) AddChunk(chunk Chunk) [][]byte {
	sequence := chunk.SequenceStart()
	if sequence < ct.horizon {
		return nil
	}
	cs, ok := ct.chunks[sequence]
	if ok && cs == nil {
		// This means that we've already returned this packet, we should not return it again.
		return nil
	}
	if !ok {
		cs = makeChunkSequencer(sequence)
		ct.chunks[sequence] = cs

		// Go through all existing sequences and remove any that are too old.
		// TODO: This is obviously inefficient, but is it terrible?  Maybe in practice this is fine.
		if sequence > ct.maxAge && sequence-ct.maxAge > ct.horizon {
			ct.horizon = sequence - ct.maxAge
		}
		var kill []SequenceId
		for sequence := range ct.chunks {
			if sequence < ct.horizon {
				kill = append(kill, sequence)
			}
		}
		for _, sequence := range kill {
			delete(ct.chunks, sequence)
		}
	}
	cs.AddChunk(&chunk)
	var ret [][]byte
	if cs.Done() {
		ret = append(ret, cs.GetPacket())
		ct.chunks[sequence] = nil
	}
	return ret
}
