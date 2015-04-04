package core

import (
	"fmt"
)

type NodeId uint16
type StreamId uint16
type SequenceId uint32

type Mode int

const (
	ModeUnreliableUnordered = iota
	ModeUnreliableDeduped
	ModeUnreliableOrdered
	ModeReliableUnordered
	ModeReliableOrdered
	ModeMax
)

func (m Mode) Sequenced() bool {
	return m.Reliable() || m.Ordered() || m.Deduped()
}
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
