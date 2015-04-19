package core

// ClientSendChunksHandler handles chunks that are sent from the user to sluice so that they can be
// dispatched.  Chunks from fromCore are sent to toHost, and if they come from a reliable stream
// they are also stored until they are truncated.  Chunks received from reserved require special
// handling.
func ClientSendChunksHandler(config *Config, fromCore, reserved <-chan Chunk, toHost chan<- Chunk) {
	pt := make(PacketTracker)
	positions := make(PositionUpdate)
	reminder := MakeStreamReminder(config.PositionChunkMin, config.PositionChunkMax, config.Clock)
	defer reminder.Close()
	for {
		select {

		// Take chunks that came directly from the user (after being chunkified from the original
		// packet) and send them to the host.  Chunks being sent on reliable streams are also
		// tracked and used to determine when to send the next position chunk.
		case chunk, ok := <-fromCore:
			if !ok {
				return
			}
			stream := config.GetStreamConfigById(chunk.Stream)
			if stream == nil {
				config.Printf("tried to send a chunk on unknown stream %d", chunk.Stream)
				break
			}
			toHost <- chunk
			if stream.Mode.Reliable() {
				pt.Add(chunk)
				reminder.Update(stream.Id)
				if chunk.Sequence > positions[stream.Id] {
					positions[stream.Id] = chunk.Sequence
				}
			}

		case chunk, ok := <-reserved:
			if !ok {
				return
			}
			switch chunk.Stream {
			case StreamResend:
				// Resend chunks are sent here from ClientRecvChunksHandler.  We immediately respond to
				// these chunks by sending all chunks mentioned in them to the host.  If we don't have one
				// of those chunks then something went horribly wrong and we will probably be disconnected
				// by the host eventually.
				req, err := ParseResendChunkData(chunk.Data)
				if err != nil {
					config.Printf("error parsing resend chunk data: %v", err)
					break
				}
				for stream, sequences := range req {
					for _, sequence := range sequences {
						if chunk := pt.Get(stream, config.Node, sequence); chunk != nil {
							toHost <- *chunk
						} else {
							config.Printf("Got a resend chunk for Stream/Sequence %d/%d, but didn't have that chunk.", stream, sequence)
						}
					}
				}

			case StreamTruncate:
				// Truncate chunks are sent here from ClientRecvChunksHandler, they let us know what chunks
				// we no longer have to track.  These chunks typically come less rapidly than other chunks
				// because the only effect of them is to free up memory.
				req, err := ParseTruncateChunkData(chunk.Data)
				if err != nil {
					config.Printf("error parsing truncate chunk data: %v", err)
					break
				}
				for stream, sequence := range req {
					pt.RemoveUpToAndIncluding(stream, config.Node, sequence)
					if !pt.ContainsAnyFor(stream, config.Node) {
						reminder.Clear(stream)
					}
				}
			}

		// The reminder triggers whenever we have chunks on a reliable stream that we haven't
		// notified the host of lately.
		case streams := <-reminder.Wait():
			p := make(PositionUpdate)
			for _, stream := range streams {
				p[stream] = positions[stream]
			}
			datas := MakePositionChunkDatas(config, p)
			for _, data := range datas {
				toHost <- Chunk{
					Stream: StreamPosition,
					Source: config.Node,
					Data:   data,
				}
			}
		}
	}
}
