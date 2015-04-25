package core

import (
	"fmt"
)

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
		panic(fmt.Sprintf("DEBUG: Chunk was from sequence %d, expected %d.", chunk.SequenceStart(), cs.sequence))
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

type unreliableChunkMerger struct {
	chunks  map[SequenceId]*chunkSequencer
	horizon SequenceId
	maxAge  SequenceId
}

func makeUnreliableChunkMerger(maxAge SequenceId) reliabilityMerger {
	if maxAge < 0 {
		maxAge = 0
	}
	return &unreliableChunkMerger{
		chunks:  make(map[SequenceId]*chunkSequencer),
		horizon: 0,
		maxAge:  maxAge,
	}
}

func (cm *unreliableChunkMerger) AddChunk(chunk Chunk) ([]byte, int) {
	sequence := chunk.SequenceStart()
	if sequence < cm.horizon {
		return nil, 0
	}
	cs, ok := cm.chunks[sequence]
	if ok && cs == nil {
		// This means that we've already returned this packet, we should not return it again.
		return nil, 0
	}
	if !ok {
		cs = makeChunkSequencer(sequence)
		cm.chunks[sequence] = cs

		// Go through all existing sequences and remove any that are too old.
		// TODO: This is obviously inefficient, but is it terrible?  Maybe in practice this is fine.
		if sequence > cm.maxAge && sequence-cm.maxAge > cm.horizon {
			cm.horizon = sequence - cm.maxAge
		}
		var kill []SequenceId
		for sequence := range cm.chunks {
			if sequence < cm.horizon {
				kill = append(kill, sequence)
			}
		}
		for _, sequence := range kill {
			delete(cm.chunks, sequence)
		}
	}
	cs.AddChunk(&chunk)
	if cs.Done() {
		cm.chunks[sequence] = nil
		return cs.GetPacket(), cs.numChunks
	}
	return nil, 0
}

type reliableChunkMerger struct {
	chunks map[SequenceId]*chunkSequencer

	// now is the earliest key in chunks
	now SequenceId
}

// reliabilityMerger is like a ChunkMerger, but doesn't care about order.  This means that the
// merger can never return more than one packet at once, which is why it returns []byte, not
// [][]byte like ChunkMerger.
type reliabilityMerger interface {
	AddChunk(chunk Chunk) ([]byte, int)
}

func makeReliableChunkMerger(start SequenceId) reliabilityMerger {
	return &reliableChunkMerger{
		chunks: make(map[SequenceId]*chunkSequencer),
		now:    start,
	}
}

func (cm *reliableChunkMerger) AddChunk(chunk Chunk) ([]byte, int) {
	sequence := chunk.SequenceStart()
	cs, ok := cm.chunks[sequence]
	if sequence < cm.now || (ok && cs.Done()) {
		// This is a duplicate chunk.
		return nil, 0
	}
	if !ok {
		cs = makeChunkSequencer(sequence)
		cm.chunks[sequence] = cs
	}
	cs.AddChunk(&chunk)
	if !cs.Done() {
		return nil, 0
	}
	for {
		cs, ok := cm.chunks[cm.now]
		if !ok || !cs.Done() {
			break
		}
		newNow := cm.now + SequenceId(cs.numChunks)
		delete(cm.chunks, cm.now)
		cm.now = newNow
	}

	packet := cs.GetPacket()
	// This is a minor optimization.  Since we might keep this sequencer around for a while while we
	// wait for earlier chunks, but since we only need the numChunks field out of it, we can nil the
	// payload so that it can be garbage collected a little earlier.
	cs.chunks = nil
	return packet, cs.numChunks
}

type unorderedMerger struct {
	rm reliabilityMerger
}

func (m *unorderedMerger) AddChunk(chunk Chunk) [][]byte {
	packet, _ := m.rm.AddChunk(chunk)
	if packet != nil {
		return [][]byte{packet}
	}
	return nil
}

// MakeUnreliableUnorderedChunkMerger returns a ChunkMerger that does not guarantee reliability or
// ordering.  maxAge is the maximum age of a chunk that it will keep before dropping it.
func MakeUnreliableUnorderedChunkMerger(maxAge SequenceId) ChunkMerger {
	return &unorderedMerger{
		rm: makeUnreliableChunkMerger(maxAge),
	}
}

// MakeReliableUnorderedChunkMerger returns a ChunkMerger that guarantees reliability, but may
// return packets out of order.
func MakeReliableUnorderedChunkMerger(start SequenceId) ChunkMerger {
	return &unorderedMerger{
		rm: makeReliableChunkMerger(start),
	}
}

type unreliableOrderedMerger struct {
	rm  reliabilityMerger
	now SequenceId
}

// MakeUnreliableOrderedChunkMerger returns a ChunkMerger that does not guarantee reliability, but
// does guarantee that packets will be delivered in chronological order.
func MakeUnreliableOrderedChunkMerger(start, maxAge SequenceId) ChunkMerger {
	return &unreliableOrderedMerger{
		rm:  makeUnreliableChunkMerger(maxAge),
		now: start,
	}
}

func (m *unreliableOrderedMerger) AddChunk(chunk Chunk) [][]byte {
	if chunk.SequenceStart() < m.now {
		return nil
	}
	packet, n := m.rm.AddChunk(chunk)
	if packet == nil {
		return nil
	}
	m.now = chunk.SequenceStart() + SequenceId(n)
	return [][]byte{packet}
}

type reliableOrderedMerger struct {
	rm      reliabilityMerger
	now     SequenceId
	packets map[SequenceId]mergedPacket
}

// MakeReliableOrderedChunkMerger returns a ChunkMerger that guarantees that all packets will arrive
// in the order they are sent.
func MakeReliableOrderedChunkMerger(start SequenceId) ChunkMerger {
	return &reliableOrderedMerger{
		rm:      makeReliableChunkMerger(start),
		now:     start,
		packets: make(map[SequenceId]mergedPacket),
	}
}

// mergedPacket is just to help track a packet along with how many chunks it took up.  This is
// needed because the mergers need to know what sequence number the next packet starts at.
type mergedPacket struct {
	packet    []byte
	numChunks int
}

func (m *reliableOrderedMerger) AddChunk(chunk Chunk) [][]byte {
	if chunk.SequenceStart() < m.now {
		return nil
	}
	packet, n := m.rm.AddChunk(chunk)
	if packet == nil {
		return nil
	}
	m.packets[chunk.SequenceStart()] = mergedPacket{packet, n}
	var ret [][]byte
	for {
		mp, ok := m.packets[m.now]
		if !ok {
			return ret
		}
		ret = append(ret, mp.packet)
		newNow := m.now + SequenceId(mp.numChunks)
		delete(m.packets, m.now)
		m.now = newNow
	}
	panic("unreachable")
}
