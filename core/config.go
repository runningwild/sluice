package core

import (
	"fmt"
	"time"

	"github.com/runningwild/clock"
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
	StreamMaxUserDefined StreamId = 1<<15 + iota

	// Note that all of the sluice-level streams always talk about chunks.  We don't want any of
	// these streams to rely on any high-level logic, so all of them always deal in individual
	// chunks and never expect packets to be reliable.

	// Confirm chunks are sent from the client to the host to confirm receipt of reliable chunks.
	StreamConfirm

	// Truncate chunks are sent from the host to the client to let the client know which chunks it
	// can forget about.
	StreamTruncate

	// Resend chunks are sent from the host to the client to ask it to resend a chunk that the host
	// never received.
	StreamResend

	// Position chunks are sent from the client to the host to let it know what chunks it should
	// have received by now.
	StreamPosition

	// Ping/Pong chunks are initiated by the host (Ping) and responded to by the client (Pong).
	StreamPing
	StreamPong

	// Ding/Dang/Dong chunks are a way for the host to gague how fast two client might be able to
	// talk to each other.  The host sends a Ding to client A with the address of client B, client A
	// sends a Dang to client B, who then sends a Dong back to the host.
	StreamDing
	StreamDang
	StreamDong

	// Punch chunks are sent from the host to a client to indicate that it should start or stop
	// sending data directly to another client.
	StreamPunch

	// Stats chunks are sent from client to the host to let it know what sort of delays it sees
	// on a ping/pong with whatever clients it is connected with.
	StreamStats

	// Join and Leave chunks are sent from the host to each client every time another client joins
	// or leaves the sluice.
	StreamJoin
	StreamLeave
)

// StreamConfig contains all the config data for a user-defined stream.
type StreamConfig struct {
	Name      string // Arbitrary name to refer to the stream by, must be unique among all streams.
	Id        StreamId
	Mode      Mode
	Broadcast bool
}

// Config contains information for how to run the sluice network.
type Config struct {
	GlobalConfig

	// NodeId of the client with this config.
	Node NodeId

	Logger Printer
}

// GlobalConfig contains the configuration for a sluice network that is constant for all nodes.
type GlobalConfig struct {
	Streams map[StreamId]StreamConfig

	// MaxChunkDataSize is the maximum size a packet can be before it is broken into chunks.
	MaxChunkDataSize int

	Clock clock.Clock

	// Min and max amount of time a client will wait between sending position chunks.
	PositionChunkMin time.Duration
	PositionChunkMax time.Duration
}

type Printer interface {
	Printf(format string, v ...interface{})
}

func (c *Config) Printf(format string, v ...interface{}) {
	if c.Logger == nil {
		return
	}
	c.Logger.Printf(format, v...)
}

func (c *Config) Validate() error {
	if c == nil || c.Streams == nil {
		return fmt.Errorf("Config and Config.Stream must both not be nil")
	}
	if c.MaxChunkDataSize < 25 || c.MaxChunkDataSize > 30000 {
		return fmt.Errorf("Config.MaxChunkDataSize must be in the range (25, 30000)")
	}
	for streamId := range c.Streams {
		if streamId == 0 {
			return fmt.Errorf("Config cannot contain streams with id == 0")
		}
		if streamId >= StreamMaxUserDefined {
			return fmt.Errorf("Config cannot contain streams with id >= %d", StreamMaxUserDefined)
		}
	}
	names := make(map[string]struct{})
	for _, stream := range c.Streams {
		if _, ok := names[stream.Name]; ok {
			return fmt.Errorf("Config cannot have two streams with the same name (%q)", stream.Name)
		}
		names[stream.Name] = struct{}{}
	}
	return nil
}

// GetIdFromName returns the StreamId of the stream with the specified name, or 0 if no such stream
// is in the config.
func (c *Config) GetIdFromName(name string) StreamId {
	for id, stream := range c.Streams {
		if stream.Name == name {
			return id
		}
	}
	return 0
}

// GetStreamConfigByName returns the StreamConfig for the specified name, or 0 if no such stream is
// in the config.
func (c *Config) GetStreamConfigByName(name string) *StreamConfig {
	for _, stream := range c.Streams {
		if stream.Name == name {
			return &stream
		}
	}
	return nil
}

// GetStreamConfigById returns the StreamConfig for the specified id, or 0 if no such stream is in
// the config.
func (c *Config) GetStreamConfigById(id StreamId) *StreamConfig {
	stream, ok := c.Streams[id]
	if !ok {
		return nil
	}
	return &stream
}
