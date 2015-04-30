package core

// HostCommunicateWithClient handles all of the communication with a single client.  Specifically
// the data being sent to and from this routine will come directly from a single client's
// ClientSendChunksHandler and ClientRecvChunksHandler routines.
// func HostRoutine(config *Config, fromOutside, fromInside <-chan RawPacket, toOutside, toInside chan<- RawPacket, c clock.Clock, ticker <-chan time.Time) {
// fromClient chunks are assembled into packets with a chunk tracker and sent to toCore
// fromCore chunks are sent immediately to toClient
func HostCommunicateWithClient(config *Config, node NodeId, fromClient, fromCore <-chan Chunk, toClient chan<- Chunk, toCore chan<- Packet) {
//	truncate := config.Clock.Tick(config.Truncate)
//	position := config.Clock.Tick(config.Truncate)
//	for {
//		select {
//		case chunk := <-fromClient:
//			if !chunk.Stream.IsReserved() {
//				stream, ok := config.GetStreamConfigById(chunk.Stream)
//				if !ok {
//					config.Printf("Got an unknown stream %d", chunk.Stream)
//					break
//				}
//				// TODO: Assemble using chunk mergers here.
//				toCore <- chunk
//				if stream.Mode.Reliable() {
//					// TODO: If this chunk indicates that a sequence is missing, we should immediately
//					// ask for a resend
//				}
//			}

//		case chunk := <-fromCore:
//			toClient <- chunk
//		}
//	}
}
