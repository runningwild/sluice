package core

import (
	"fmt"
)

// SequenceTracker is a simple way of keeping track of what SequenceIds have been received on a
// particular stream.
type SequenceTracker struct {
	// The stream that this SequenceTracker is tracking.
	stream StreamId

	// The node that is sending these chunks.
	node NodeId

	// The value of maxContiguous is the highest SequenceId such that all chunks with an equal or
	// lesser SequenceId have been received.
	maxContiguous SequenceId

	// others contains all of the SequenceIds of chunks that have been received and are greater than
	// maxContiguous by at least 2.
	others map[SequenceId]bool
}

// MakeSequenceTracker returns a SequenceTracker for the specified stream/node.  It will start
// tracking at start, i.e. start is the first sequence that it doesn't have yet.
func MakeSequenceTracker(stream StreamId, node NodeId, start SequenceId) *SequenceTracker {
	return &SequenceTracker{
		stream:        stream,
		node:          node,
		maxContiguous: start - 1,
		others:        make(map[SequenceId]bool),
	}
}

// AddSequenceId adds id to the set of sequence ids that have been tracked by this tracker.
func (st *SequenceTracker) AddSequenceId(id SequenceId) {
	st.others[id] = true
	for next := st.maxContiguous + 1; st.others[next]; next++ {
		st.maxContiguous = next
		delete(st.others, next)
	}
}

// StreamId returns the stream id of the stream associated with this tracker.
func (st *SequenceTracker) StreamId() StreamId {
	return st.stream
}

// NodeId returns the node id of the node associated with this tracker.
func (st *SequenceTracker) NodeId() NodeId {
	return st.node
}

// Contains returns true iff this tracker has tracked id.
func (st *SequenceTracker) Contains(id SequenceId) bool {
	if id <= st.maxContiguous {
		return true
	}
	return st.others[id]
}

// ContainsAllUpTo returns true iff this tracker has tracked all ids up to and including id.
func (st *SequenceTracker) ContainsAllUpTo(id SequenceId) bool {
	return id <= st.maxContiguous
}

func (st *SequenceTracker) String() string {
	ret := fmt.Sprintf("StreamId: %d\n", st.stream)
	ret += fmt.Sprintf("NodeId: %d\n", st.node)
	ret += fmt.Sprintf("MaxContiguous: %d\n", st.maxContiguous)
	ret += fmt.Sprintf("Others: %v\n", st.others)
	return ret
}

// Returns a []byte in the following format:
// <16:stream id><16:node id><16:num ids><16:max contiguous>[for each other id: <16:id>]
func AppendSequenceTracker(data []byte, st *SequenceTracker) []byte {
	data = AppendStreamId(data, st.stream)
	data = AppendNodeId(data, st.node)
	data = AppendUint16(data, uint16(len(st.others)+1))
	data = AppendSequenceId(data, st.maxContiguous)
	for sequence := range st.others {
		data = AppendSequenceId(data, sequence)
	}
	return data
}

// DeserializeSequenceTracker creates a SequenceTracker from data and returns the number of bytes
// read and the sequence tracker.
func ConsumeSequenceTracker(data []byte, st *SequenceTracker) ([]byte, error) {
	if len(data) < 8 {
		return data, fmt.Errorf("Invalid data stream.")
	}
	data = ConsumeStreamId(data, &st.stream)
	data = ConsumeNodeId(data, &st.node)
	var numIds uint16
	data = ConsumeUint16(data, &numIds)
	if len(data) < int(4*numIds) {
		return data, fmt.Errorf("Invalid data stream.")
	}
	data = ConsumeSequenceId(data, &st.maxContiguous)
	st.others = make(map[SequenceId]bool, numIds-1)
	for ; numIds > 1; numIds-- {
		var sequence SequenceId
		data = ConsumeSequenceId(data, &sequence)
		st.others[sequence] = true
	}
	return data, nil
}
