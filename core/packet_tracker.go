package core

type streamNodeId struct {
	stream StreamId
	node   NodeId
}
type PacketTracker map[streamNodeId]map[SequenceId]Chunk

// Add adds chunk to the tracker.
func (pt PacketTracker) Add(chunk Chunk) {
	snid := streamNodeId{chunk.Stream, chunk.Source}
	if _, ok := pt[snid]; !ok {
		pt[snid] = make(map[SequenceId]Chunk)
	}
	pt[snid][chunk.Sequence] = chunk
}

// Remove removes the chunk at stream/node/sequence from the tracker.
func (pt PacketTracker) Remove(stream StreamId, node NodeId, sequence SequenceId) {
	snid := streamNodeId{stream, node}
	delete(pt[snid], sequence)
	if len(pt[snid]) == 0 {
		delete(pt, snid)
	}
}

// Get returns the chunk at stream/node/sequence, or nil if there is no Chunk there.
func (pt PacketTracker) Get(stream StreamId, node NodeId, sequence SequenceId) *Chunk {
	snid := streamNodeId{stream, node}
	if !pt.ContainsAnyFor(stream, node) {
		return nil
	}
	chunk, ok := pt[snid][sequence]
	if !ok {
		return nil
	}
	return &chunk
}

// RemoveUpToAndIncluding removes all chunks on the stream/node that have a SequenceId less than or
// equal to sequence.
func (pt PacketTracker) RemoveUpToAndIncluding(stream StreamId, node NodeId, sequence SequenceId) {
	snid := streamNodeId{stream, node}
	var sequences []SequenceId
	for s := range pt[snid] {
		if s <= sequence {
			sequences = append(sequences, s)
		}
	}
	for _, s := range sequences {
		pt.Remove(stream, node, s)
	}
}

// RemoveSequenceTracked removes from the PacketTracker all chunks tracked in SequenceTracker.
func (pt PacketTracker) RemoveSequenceTracked(st *SequenceTracker) {
	snid := streamNodeId{st.StreamId(), st.NodeId()}
	var remove []SequenceId
	for sequence := range pt[snid] {
		if st.Contains(sequence) {
			remove = append(remove, sequence)
		}
	}
	for _, sequence := range remove {
		pt.Remove(snid.stream, snid.node, sequence)
	}
}

// ContainsAnyFor returns true iff the PacketTracker currently has any chunks on stream/node.
func (pt PacketTracker) ContainsAnyFor(stream StreamId, node NodeId) bool {
	snid := streamNodeId{stream, node}
	_, present := pt[snid]
	return present
}

// Contains returns true iff the PacketTracker has a chunk at stream/node/sequence.
func (pt PacketTracker) Contains(stream StreamId, node NodeId, sequence SequenceId) bool {
	snid := streamNodeId{stream, node}
	if !pt.ContainsAnyFor(stream, node) {
		return false
	}
	_, ok := pt[snid][sequence]
	return ok
}
