package core

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
