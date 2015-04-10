package core

import (
	"fmt"
)

// NodeId is used to identify clients.  The Host has NodeId == 1.  NodeIds are not reused, so it a
// client disconnects and then reconnects it will be assigned a new, previously unused, NodeId.
type NodeId uint16

// StreamId is used to distinguish different streams.  Users identify streams with arbitrary
// strings, and those are assigned StreamIds.  There are also reserved StreamIds for all of the
// low-level data that sluice needs to send.
type StreamId uint16

// SequenceId is used to order chunks within a stream.  Even unordered and unreliable streams use
// SequenceIds, they are necessary for reassembling chunked packets.  The SequenceId of streams
// starts at 1 and is incremented for each chunk.
type SequenceId uint32

// SubsequenceIndex is used to order chunks that all came from the same packet.  If a packet did not
// get split into multiple packets the SubsequenceIndex will be 0, otherwise the first chunk in that
// packet will be 1, and the index will be incremented for each successive chunk.  Note that for a
// packet that is split into multiple chunks the SequenceId and SubsequenceIndex will both increment
// from each chunk in the sequence to the next.
type SubsequenceIndex uint16

// Mode defines what kind of reliability is expected on a stream.  Regardless of the mode, all
// packets that do arrive will be subject to a CRC, and will be reassembled into their original
// length.  Duplicate packets are also removed for all modes.
type Mode int

const (
	// ModeUnreliableUnordered indicates that packets in a stream can arrive out of order and any
	// packets that are dropped will not be resent.  This is the mode that is most similar to UDP
	// and has the least overhead.
	ModeUnreliableUnordered = iota

	// ModeUnreliableOrdered indicates that packets may be dropped, but packets that do arrive will
	// be received in the order that they were sent.  This means that a packet may technically
	// arrive late and we will reject it because a new packet has already arrived and we don't want
	// anything on this stream to be received out of order.
	ModeUnreliableOrdered

	// ModeReliableUnordered indicates that all packets on this stream must be received, but that
	// they may be received out of order.
	ModeReliableUnordered

	// ModeReliableOrdered indicates that all packets on this stream must be received, and they must
	// be received in the order they were sent.  This is the mode that is most similar to TCP.
	ModeReliableOrdered

	ModeMax
)

func (m Mode) Reliable() bool {
	return m == ModeReliableOrdered || m == ModeReliableUnordered
}
func (m Mode) Ordered() bool {
	return m == ModeUnreliableOrdered || m == ModeReliableOrdered
}
func (m Mode) Deduped() bool {
	return m != ModeUnreliableUnordered
}

const (
	streamMaxUserDefined StreamId = 1<<15 + iota

	// Confirm packets are sent from the client to the host to confirm receipt of reliable packets.
	streamConfirm

	// Truncate packets are sent from the host to the client to let the client know which packets
	// it can forget about.
	streamTruncate

	// Resend packets are sent from the host to the client to ask it to resend a packet that it
	// never received.
	streamResend

	// Position packets are sent from the client to the host to let it know what packets it should
	// have received by now.
	streamPosition

	// Ping and Pong packets are sent between any two connected nodes to gauge the round trip time
	// between those nodes.
	streamPing
	streamPong

	// Punch packets are sent from the host to a client to indicate that it should start or stop
	// sending data directly to another client.
	streamPunch

	// Stats packets are sent from client to the host to let it know what sort of delays it sees
	// on a ping/pong with whatever clients it is connected with.
	streamStats

	// Join and Leave packets are sent from the host to each client every time another client joins
	// or leaves the sluice.
	streamJoin
	streamLeave
)

// StreamConfig contains all the config data for a user-defined stream.
type StreamConfig struct {
	Name      string // Arbitrary name to refer to the stream by, must be unique among all streams.
	Id        StreamId
	Mode      Mode
	Broadcast bool
}

// Config contains the set of all user-defined streams.  It is read-only after creation, so it is
// safe for concurrent access.
type Config map[StreamId]StreamConfig

func (c Config) Validate() error {
	if c == nil {
		return fmt.Errorf("Config cannot be nil")
	}
	for streamId := range c {
		if streamId == 0 {
			return fmt.Errorf("Config cannot contain streams with id == 0")
		}
		if streamId >= streamMaxUserDefined {
			return fmt.Errorf("Config cannot contain streams with id >= %d", streamMaxUserDefined)
		}
	}
	names := make(map[string]struct{})
	for _, stream := range c {
		if _, ok := names[stream.Name]; ok {
			return fmt.Errorf("Config cannot have two streams with the same name (%q)", stream.Name)
		}
		names[stream.Name] = struct{}{}
	}
	return nil
}

// GetIdFromName returns the StreamId of the stream with the specified name, or 0 if no such stream
// is in the config.
func (c Config) GetIdFromName(name string) StreamId {
	for id, stream := range c {
		if stream.Name == name {
			return id
		}
	}
	return 0
}

// GetStreamConfigByName returns the StreamConfig for the specified name, or 0 if no such stream is
// in the config.
func (c Config) GetStreamConfigByName(name string) *StreamConfig {
	for _, stream := range c {
		if stream.Name == name {
			return &stream
		}
	}
	return nil
}

// GetStreamConfigById returns the StreamConfig for the specified id, or 0 if no such stream is in
// the config.
func (c Config) GetStreamConfigById(id StreamId) *StreamConfig {
	stream, ok := c[id]
	if !ok {
		return nil
	}
	return &stream
}
