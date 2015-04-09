package core

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
		sequence++
		if len(packet) <= maxChunkDataSize {
			chunks <- Chunk{
				Source:      0, // Irrelevant unless being sent from the host
				Target:      target,
				Stream:      config.Id,
				Sequence:    sequence,
				Subsequence: 0,
				Data:        packet,
			}
			continue
		}

		// This will break packet into chunks such that len(chunk.Data) <= maxChunkDataSize.
		// A zero-length chunk will be appended if the last chunk length would otherwise have been
		// exactly maxChunkDataSize.  This way the last chunk in a sequence can be detected by
		// checking its size.
		var index SubsequenceIndex = 0
		lastSend := -1
		for len(packet) > 0 && lastSend < maxChunkDataSize {
			index++
			chunkData := packet
			if len(chunkData) > maxChunkDataSize {
				chunkData = chunkData[0:maxChunkDataSize]
			}
			chunks <- Chunk{
				Source:   0, // Irrelevant unless being sent from the host
				Target:   target,
				Stream:   config.Id,
				Sequence: sequence,
				Data:     chunkData,
			}
			packet = packet[len(chunkData):]
			lastSend = len(chunkData)
		}
	}
}

type chunkSequencer struct {
	chunks    map[SubsequenceIndex]*Chunk
	lastChunk *Chunk
	numChunks int
	sequence  SequenceId
}

func makeChunkSequencer(sequence SequenceId) *chunkSequencer {
	return &chunkSequencer{
		chunks:   make(map[SubsequenceIndex]*Chunk),
		sequence: sequence,
	}
}

func (cs *chunkSequencer) AddChunk(chunk *Chunk) {
	if chunk.Sequence != cs.sequence {
		panic("Chunk was added to the wrong chunkSequencer.")
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

// ChunkTracker provides a way of tracking incoming chunks and assembling them into packets.
type ChunkTracker interface {
	// AddChunk adds chunk to the tracker.  Any packets that are available after this addition are
	// returned in the order they were sent.
	AddChunk(chunk Chunk) [][]byte
}

type UnreliableUnorderedChunkTracker struct {
	chunks  map[SequenceId]*chunkSequencer
	horizon SequenceId
	maxAge  SequenceId
}

func MakeUnreliableUnorderedChunkTracker(maxAge SequenceId) ChunkTracker {
	if maxAge < 0 {
		maxAge = 0
	}
	return &UnreliableUnorderedChunkTracker{
		chunks:  make(map[SequenceId]*chunkSequencer),
		horizon: 0,
		maxAge:  maxAge,
	}
}
func (ct *UnreliableUnorderedChunkTracker) AddChunk(chunk Chunk) [][]byte {
	if chunk.Sequence < ct.horizon {
		return nil
	}
	cs, ok := ct.chunks[chunk.Sequence]
	if ok && cs == nil {
		// This means that we've already returned this packet, we should not return it again.
		return nil
	}
	if !ok {
		cs = makeChunkSequencer(chunk.Sequence)
		ct.chunks[chunk.Sequence] = cs

		// Go through all existing sequences and remove any that are too old.
		// TODO: This is obviously inefficient, but is it terrible?  Maybe in practice this is fine.
		if chunk.Sequence > ct.maxAge && chunk.Sequence-ct.maxAge > ct.horizon {
			ct.horizon = chunk.Sequence - ct.maxAge
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
		ct.chunks[chunk.Sequence] = nil
	}
	return ret
}
