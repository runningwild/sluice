package core_test

import (
	"github.com/runningwild/clock"
	"github.com/runningwild/cmwc"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/runningwild/sluice/core"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestClientSendChunks(t *testing.T) {
	Convey("ClientSendChunksHandler", t, func() {
		config := &core.Config{
			Node:   5,
			Logger: log.New(os.Stdout, "", log.Lshortfile|log.Ltime),
			GlobalConfig: core.GlobalConfig{
				Streams: map[core.StreamId]core.StreamConfig{
					7: core.StreamConfig{
						Name: "UU",
						Id:   7,
						Mode: core.ModeUnreliableUnordered,
					},
					8: core.StreamConfig{
						Name: "UO",
						Id:   8,
						Mode: core.ModeUnreliableOrdered,
					},
					9: core.StreamConfig{
						Name: "RU",
						Id:   9,
						Mode: core.ModeReliableUnordered,
					},
					10: core.StreamConfig{
						Name: "RO",
						Id:   10,
						Mode: core.ModeReliableOrdered,
					},
				},
				MaxChunkDataSize: 50,
				PositionChunkMin: 20 * time.Millisecond,
				PositionChunkMax: 50 * time.Millisecond,
				Clock:            &clock.RealClock{},
			},
		}
		fromCore := make(chan core.Chunk)
		reserved := make(chan core.Chunk)
		toHost := make(chan core.Chunk)
		handlerIsDone := make(chan struct{})
		defer func() {
			close(fromCore)
			close(reserved)
			for {
				select {
				case <-handlerIsDone:
					return
				case <-toHost:
				}
			}
		}()
		go func() {
			core.ClientSendChunksHandler(config, fromCore, reserved, toHost)
			close(handlerIsDone)
		}()

		Convey("Copies all fromCore chunks to toHost.", func() {
			go func() {
				fromCore <- makeSimpleChunk(config.GetIdFromName("UU"), config.Node, 10)
				fromCore <- makeSimpleChunk(config.GetIdFromName("UU"), config.Node, 11)
				fromCore <- makeSimpleChunk(config.GetIdFromName("UU"), config.Node, 12)
				fromCore <- makeSimpleChunk(config.GetIdFromName("UO"), config.Node, 20)
				fromCore <- makeSimpleChunk(config.GetIdFromName("UO"), config.Node, 21)
				fromCore <- makeSimpleChunk(config.GetIdFromName("UO"), config.Node, 22)
				fromCore <- makeSimpleChunk(config.GetIdFromName("RU"), config.Node, 30)
				fromCore <- makeSimpleChunk(config.GetIdFromName("RU"), config.Node, 31)
				fromCore <- makeSimpleChunk(config.GetIdFromName("RU"), config.Node, 32)
				fromCore <- makeSimpleChunk(config.GetIdFromName("RO"), config.Node, 40)
				fromCore <- makeSimpleChunk(config.GetIdFromName("RO"), config.Node, 41)
				fromCore <- makeSimpleChunk(config.GetIdFromName("RO"), config.Node, 42)
			}()
			pt := make(core.PacketTracker)
			for i := 0; i < 12; i++ {
				pt.Add(<-toHost)
			}
			So(verifySimpleChunk(pt.Get(config.GetIdFromName("UU"), config.Node, 10)), ShouldBeTrue)
			So(verifySimpleChunk(pt.Get(config.GetIdFromName("UU"), config.Node, 11)), ShouldBeTrue)
			So(verifySimpleChunk(pt.Get(config.GetIdFromName("UU"), config.Node, 12)), ShouldBeTrue)
			So(verifySimpleChunk(pt.Get(config.GetIdFromName("UO"), config.Node, 20)), ShouldBeTrue)
			So(verifySimpleChunk(pt.Get(config.GetIdFromName("UO"), config.Node, 21)), ShouldBeTrue)
			So(verifySimpleChunk(pt.Get(config.GetIdFromName("UO"), config.Node, 22)), ShouldBeTrue)
			So(verifySimpleChunk(pt.Get(config.GetIdFromName("RU"), config.Node, 30)), ShouldBeTrue)
			So(verifySimpleChunk(pt.Get(config.GetIdFromName("RU"), config.Node, 31)), ShouldBeTrue)
			So(verifySimpleChunk(pt.Get(config.GetIdFromName("RU"), config.Node, 32)), ShouldBeTrue)
			So(verifySimpleChunk(pt.Get(config.GetIdFromName("RO"), config.Node, 40)), ShouldBeTrue)
			So(verifySimpleChunk(pt.Get(config.GetIdFromName("RO"), config.Node, 41)), ShouldBeTrue)
			So(verifySimpleChunk(pt.Get(config.GetIdFromName("RO"), config.Node, 42)), ShouldBeTrue)
		})

		Convey("After it gets a bunch of chunks on reliable streams.", func() {
			N := 100
			go func() {
				for i := 0; i < N; i++ {
					fromCore <- makeSimpleChunk(config.GetIdFromName("RU"), config.Node, core.SequenceId(i))
					fromCore <- makeSimpleChunk(config.GetIdFromName("RO"), config.Node, core.SequenceId(i))
				}
			}()
			for i := 0; i < N; i++ {
				<-toHost
				<-toHost
			}

			Convey("Sends position chunks periodically.", func() {
				done := make(chan struct{})
				go func() {
					time.Sleep(200 * time.Millisecond)
					close(done)
				}()
				count := 0

			getPositions:
				for {
					select {
					case <-done:
						So(count, ShouldBeGreaterThan, 2)
						So(count, ShouldBeLessThan, 10)
						break getPositions

					case chunk := <-toHost:
						count++
						positions, err := core.ParsePositionChunkData(chunk.Data)
						So(err, ShouldBeNil)
						So(len(positions), ShouldEqual, 2)
						So(positions[config.GetIdFromName("RU")], ShouldEqual, core.SequenceId(N-1))
						So(positions[config.GetIdFromName("RO")], ShouldEqual, core.SequenceId(N-1))
					}
				}

				Convey("Unless it is sending chunks anyway.", func() {
					status := make(chan struct{})
					// Defer the close because if a check fails in here we'll get a confusing panic
					// otherwise.
					defer close(status)

					// Send chunks periodically on one of the reliable streams, that one should not
					// cause any position chunks to be sent.  After a while, stop sending those
					// chunks, this will cause the positions chunks to start sending again.
					go func() {
						next := core.SequenceId(N)
						timeout := time.After(100 * time.Millisecond)
						for {
							select {
							case <-status:
								return

							case <-timeout:
								status <- struct{}{}
								time.Sleep(100 * time.Millisecond)
								status <- struct{}{}
								return

							case <-time.After(10 * time.Millisecond):
								fromCore <- makeSimpleChunk(config.GetIdFromName("RU"), config.Node, next)
								next++
							}
						}
					}()

					var countRO, countRU int
				getOnePosition:
					for {
						select {
						case <-status:
							// Might get 1 chunk because we don't start this phase of the test
							// immediately.
							So(countRU, ShouldBeLessThanOrEqualTo, 1)
							So(countRO, ShouldBeGreaterThan, 1)
							So(countRO, ShouldBeLessThan, 5)
							break getOnePosition

						case chunk := <-toHost:
							if chunk.Stream != core.StreamPosition {
								// The chunks we're sending will come through here, but we don't
								// want to parse them like a position packet.
								break
							}
							positions, err := core.ParsePositionChunkData(chunk.Data)
							So(err, ShouldBeNil)
							So(len(positions), ShouldBeGreaterThanOrEqualTo, 1)
							So(len(positions), ShouldBeLessThanOrEqualTo, 2)
							if position, ok := positions[config.GetIdFromName("RU")]; ok {
								countRU++
								So(position, ShouldBeGreaterThanOrEqualTo, core.SequenceId(N-1))
							}
							if position, ok := positions[config.GetIdFromName("RO")]; ok {
								countRO++
								So(position, ShouldEqual, core.SequenceId(N-1))
							}
						}
					}

					countRO, countRU = 0, 0
				getBothPosition:
					for {
						select {
						case <-status:
							// Should be getting chunks for both of the reliable streams.
							So(countRU, ShouldBeGreaterThan, 1)
							So(countRU, ShouldBeLessThan, 5)
							So(countRO, ShouldBeGreaterThan, 1)
							So(countRO, ShouldBeLessThan, 5)
							break getBothPosition

						case chunk := <-toHost:
							if chunk.Stream != core.StreamPosition {
								// The chunks we're sending will come through here, but we don't
								// want to parse them like a position packet.
								break
							}
							positions, err := core.ParsePositionChunkData(chunk.Data)
							So(err, ShouldBeNil)
							So(len(positions), ShouldBeGreaterThanOrEqualTo, 1)
							So(len(positions), ShouldBeLessThanOrEqualTo, 2)
							if position, ok := positions[config.GetIdFromName("RU")]; ok {
								countRU++
								So(position, ShouldBeGreaterThanOrEqualTo, core.SequenceId(N-1))
							}
							if position, ok := positions[config.GetIdFromName("RO")]; ok {
								countRO++
								So(position, ShouldEqual, core.SequenceId(N-1))
							}
						}
					}

				})
				Convey("Until the streams are truncated.", func() {
					// Truncate one stream.
					go func() {
						req := core.TruncateRequest{
							config.GetIdFromName("RU"): core.SequenceId(N - 1),
						}
						for _, data := range core.MakeTruncateChunkDatas(config, req) {
							reserved <- core.Chunk{
								Stream: core.StreamTruncate,
								Source: 1,
								Data:   data,
							}
						}
					}()

					// Wait long enough to get some position packets.
					done := make(chan struct{})
					go func() {
						time.Sleep(200 * time.Millisecond)
						close(done)
					}()
					count := 0
					for {
						select {
						case <-done:
							So(count, ShouldBeGreaterThan, 1)
							return

						case chunk := <-toHost:
							if chunk.Stream != core.StreamPosition {
								// The chunks we're sending will come through here, but we don't
								// want to parse them like a position packet.
								break
							}
							positions, err := core.ParsePositionChunkData(chunk.Data)
							So(err, ShouldBeNil)
							So(len(positions), ShouldBeGreaterThanOrEqualTo, 1)
							So(len(positions), ShouldBeLessThanOrEqualTo, 2)
							if len(positions) == 1 {
								count++
								So(positions[config.GetIdFromName("RO")], ShouldEqual, core.SequenceId(N-1))
							}
						}
					}
				})
			})

			Convey("Resends chunks when requested.", func() {
				go func() {
					// Request a resend of all even chunks on RU and all odd chunks on RO.
					req := make(core.ResendRequest)
					for i := 0; i < N; i++ {
						var id core.StreamId
						if i%2 == 0 {
							id = config.GetIdFromName("RU")
						} else {
							id = config.GetIdFromName("RO")
						}
						req[id] = append(req[id], core.SequenceId(i))
					}
					for _, data := range core.MakeResendChunkDatas(config, req) {
						reserved <- core.Chunk{
							Stream: core.StreamResend,
							Source: 1,
							Data:   data,
						}
					}
				}()

				pt := make(core.PacketTracker)
				for i := 0; i < N; i++ {
					pt.Add(<-toHost)
				}
				for i := 0; i < N; i++ {
					if i%2 == 0 {
						So(verifySimpleChunk(pt.Get(config.GetIdFromName("RU"), config.Node, core.SequenceId(i))), ShouldBeTrue)
						So(pt.Get(config.GetIdFromName("RO"), config.Node, core.SequenceId(i)), ShouldBeNil)
					} else {
						So(verifySimpleChunk(pt.Get(config.GetIdFromName("RO"), config.Node, core.SequenceId(i))), ShouldBeTrue)
						So(pt.Get(config.GetIdFromName("RU"), config.Node, core.SequenceId(i)), ShouldBeNil)
					}
				}
			})

			Convey("Then gets a truncate request.", func() {
				go func() {
					req := core.TruncateRequest{
						config.GetIdFromName("RU"): 30,
						config.GetIdFromName("RO"): 90,
					}
					for _, data := range core.MakeTruncateChunkDatas(config, req) {
						reserved <- core.Chunk{
							Stream: core.StreamTruncate,
							Source: 1,
							Data:   data,
						}
					}
				}()

				// This is a strange test, since this shouldn't happen in practice, but I don't
				// know any other reasonable way to check if data has actually been truncated,
				// other than checking memory usage.
				Convey("Can only respond to resend requests after the truncation point.", func() {
					go func() {
						req := core.ResendRequest{
							config.GetIdFromName("RU"): []core.SequenceId{20, 30, 40},
							config.GetIdFromName("RO"): []core.SequenceId{60, 70, 80},
						}
						for _, data := range core.MakeResendChunkDatas(config, req) {
							reserved <- core.Chunk{
								Stream: core.StreamResend,
								Source: 1,
								Data:   data,
							}
						}
					}()
					pt := make(core.PacketTracker)
					for i := 0; i < 1; i++ {
						pt.Add(<-toHost)
					}
					So(pt.Get(config.GetIdFromName("RU"), config.Node, 20), ShouldBeNil)
					So(pt.Get(config.GetIdFromName("RU"), config.Node, 30), ShouldBeNil)
					So(verifySimpleChunk(pt.Get(config.GetIdFromName("RU"), config.Node, 40)), ShouldBeTrue)
					So(pt.Get(config.GetIdFromName("RO"), config.Node, 60), ShouldBeNil)
					So(pt.Get(config.GetIdFromName("RO"), config.Node, 70), ShouldBeNil)
					So(pt.Get(config.GetIdFromName("RO"), config.Node, 80), ShouldBeNil)
				})
			})
		})
	})
}

func TestClientRecvChunks(t *testing.T) {
	Convey("ClientRecvChunksHandler", t, func() {
		config := &core.Config{
			Node:   5,
			Logger: log.New(os.Stdout, "", log.Lshortfile|log.Ltime),
			GlobalConfig: core.GlobalConfig{
				Streams: map[core.StreamId]core.StreamConfig{
					7: core.StreamConfig{
						Name: "UU",
						Id:   7,
						Mode: core.ModeUnreliableUnordered,
					},
					8: core.StreamConfig{
						Name: "UO",
						Id:   8,
						Mode: core.ModeUnreliableOrdered,
					},
					9: core.StreamConfig{
						Name: "RU",
						Id:   9,
						Mode: core.ModeReliableUnordered,
					},
					10: core.StreamConfig{
						Name: "RO",
						Id:   10,
						Mode: core.ModeReliableOrdered,
					},
				},
				MaxChunkDataSize: 50,
				MaxUnreliableAge: 25,
				Confirmation:     10 * time.Millisecond,
				Clock:            &clock.RealClock{},
			},
			Starts: map[core.Streamlet]core.SequenceId{
				core.Streamlet{9, 777}:  5,
				core.Streamlet{10, 777}: 5,
				core.Streamlet{9, 778}:  25,
				core.Streamlet{10, 778}: 25,
			},
		}
		fromHost := make(chan core.Chunk)
		toCore := make(chan core.Packet)
		toHost := make(chan core.Chunk)
		reserved := make(chan core.Chunk)
		handlerIsDone := make(chan struct{})
		defer func() {
			close(fromHost)
			for {
				select {
				case <-handlerIsDone:
					close(toHost)
					close(toCore)
					return
				case <-toHost:
				case <-toCore:
				}
			}
		}()
		go func() {
			core.ClientRecvChunksHandler(config, fromHost, toCore, toHost, reserved)
			close(handlerIsDone)
		}()

		Convey("Unreserved chunks from fromHost get assembled into packets and sent to toCore.", func() {
			go func() {
				// This is just to make sure the routine doesn't block trying to send to the host.
				for range toHost {
				}
			}()
			go func() {
				var chunks []core.Chunk
				// The first packet is before the start of the stream, it should not get reported.
				chunks = append(chunks, makeChunks(config, config.GetIdFromName("RO"), 777, 0, 5)...)
				chunks = append(chunks, makeChunks(config, config.GetIdFromName("RO"), 777, 5, 5)...)
				chunks = append(chunks, makeChunks(config, config.GetIdFromName("RO"), 778, 25, 5)...)
				chunks = append(chunks, makeChunks(config, config.GetIdFromName("RO"), 777, 10, 5)...)
				for _, chunk := range chunks {
					fromHost <- chunk
				}
			}()
			So(verifyPacket((<-toCore).Data, config.GetIdFromName("RO"), 777, 5, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, config.GetIdFromName("RO"), 778, 25, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, config.GetIdFromName("RO"), 777, 10, 5), ShouldBeTrue)
		})

		Convey("Reserved chunks from fromHost get sent directly through the reserved channel.", func() {
			go func() {
				// This is just to make sure the routine doesn't block trying to send to the host.
				for range toHost {
				}
			}()
			go func() {
				fromHost <- makeSimpleChunk(core.StreamTruncate, 1, 10)
				fromHost <- makeSimpleChunk(core.StreamPing, 1, 11)
				fromHost <- makeSimpleChunk(core.StreamDing, 1, 100)
				fromHost <- makeSimpleChunk(core.StreamResend, 1, 20)
			}()
			for i := 0; i < 4; i++ {
				chunk := <-reserved
				So(verifySimpleChunk(&chunk), ShouldBeTrue)
			}
			select {
			case <-toCore:
				panic("Should not have gotten anything from <-toCore")
			default:
			}
		})

		Convey("Unreliable and Unordered streams might drop packets, and might deliver them out of order.", func() {
			go func() {
				// This is just to make sure the routine doesn't block trying to send to the host.
				for range toHost {
				}
			}()
			var node core.NodeId = 777
			stream := config.GetIdFromName("UU")
			go func() {
				var chunks []core.Chunk
				// Send all but the first chunk for each packet.  Note that the last packet is more
				// than MaxAge in the future from the first packet.
				chunkSets := [][]core.Chunk{
					// The first packet is before the start of the stream, it should not get reported.
					makeChunks(config, stream, node, 0, 5),
					makeChunks(config, stream, node, 5, 5),
					makeChunks(config, stream, node, 10, 5),
					makeChunks(config, stream, node, 15, 5),
					makeChunks(config, stream, node, 25, 5),
					makeChunks(config, stream, node, 35, 5),
				}
				for _, chunkSet := range chunkSets {
					chunks = append(chunks, chunkSet[0])
					for j := 1; j < len(chunkSet); j++ {
						fromHost <- chunkSet[j]
					}
				}
				// Finish the packets in this order: 2,3,0,1,5,4.  We should receive, in order, 2,3,5,4
				for _, index := range []int{2, 3, 0, 1, 5, 4} {
					fromHost <- chunks[index]
				}
			}()
			So(verifyPacket((<-toCore).Data, stream, node, 10, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, stream, node, 15, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, stream, node, 35, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, stream, node, 25, 5), ShouldBeTrue)
		})

		Convey("Unreliable and Ordered streams might drop packets, but all packets that are received are in order.", func() {
			go func() {
				// This is just to make sure the routine doesn't block trying to send to the host.
				for range toHost {
				}
			}()
			var node core.NodeId = 777
			stream := config.GetIdFromName("UO")
			go func() {
				var chunks []core.Chunk
				// Send all but the first chunk for each packet
				chunkSets := [][]core.Chunk{
					// The first packet is before the start of the stream, it should not get reported.
					makeChunks(config, stream, node, 0, 5),
					makeChunks(config, stream, node, 5, 5),
					makeChunks(config, stream, node, 10, 5),
					makeChunks(config, stream, node, 15, 5),
					makeChunks(config, stream, node, 20, 5),
					makeChunks(config, stream, node, 25, 5),
				}
				for _, chunkSet := range chunkSets {
					chunks = append(chunks, chunkSet[0])
					for j := 1; j < len(chunkSet); j++ {
						fromHost <- chunkSet[j]
					}
				}
				// Finish the packets in this order: 2,3,0,1,5,4.  We should receive, in order, 2,3,5,4
				for _, index := range []int{2, 3, 0, 1, 5, 4} {
					fromHost <- chunks[index]
				}
			}()
			So(verifyPacket((<-toCore).Data, stream, node, 10, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, stream, node, 15, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, stream, node, 25, 5), ShouldBeTrue)
		})

		Convey("Reliable and Unordered streams produce all packets, but can produce them out of order.", func() {
			go func() {
				// This is just to make sure the routine doesn't block trying to send to the host.
				for range toHost {
				}
			}()
			var node core.NodeId = 777
			stream := config.GetIdFromName("RU")
			go func() {
				var chunks []core.Chunk
				// The first packet is before the start of the stream, it should not get reported.
				chunks = append(chunks, makeChunks(config, stream, node, 0, 5)...)
				chunks = append(chunks, makeChunks(config, stream, node, 5, 5)...)
				chunks = append(chunks, makeChunks(config, stream, node, 10, 5)...)
				chunks = append(chunks, makeChunks(config, stream, node, 15, 5)...)
				chunks = append(chunks, makeChunks(config, stream, node, 20, 5)...)
				chunks = append(chunks, makeChunks(config, stream, node, 25, 5)...)
				// Send them in reverse order
				for i := range chunks {
					fromHost <- chunks[len(chunks)-1-i]
				}
			}()
			So(verifyPacket((<-toCore).Data, stream, node, 25, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, stream, node, 20, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, stream, node, 15, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, stream, node, 10, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, stream, node, 5, 5), ShouldBeTrue)
		})

		Convey("Reliable and Ordered streams produce all packets in order.", func() {
			go func() {
				// This is just to make sure the routine doesn't block trying to send to the host.
				for range toHost {
				}
			}()
			var node core.NodeId = 777
			stream := config.GetIdFromName("RO")
			go func() {
				var chunks []core.Chunk
				chunks = append(chunks, makeChunks(config, stream, node, 5, 5)...)
				chunks = append(chunks, makeChunks(config, stream, node, 10, 5)...)
				chunks = append(chunks, makeChunks(config, stream, node, 15, 5)...)
				chunks = append(chunks, makeChunks(config, stream, node, 20, 5)...)
				chunks = append(chunks, makeChunks(config, stream, node, 25, 5)...)
				// Send them in reverse order
				for i := range chunks {
					fromHost <- chunks[len(chunks)-1-i]
				}
			}()
			So(verifyPacket((<-toCore).Data, stream, node, 5, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, stream, node, 10, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, stream, node, 15, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, stream, node, 20, 5), ShouldBeTrue)
			So(verifyPacket((<-toCore).Data, stream, node, 25, 5), ShouldBeTrue)
		})

		Convey("Confirmation packets are sent at regular intervals.", func() {
			go func() {
				// This is just to make sure the routine doesn't block trying to send to the core.
				for range toCore {
				}
			}()
			config.MaxChunkDataSize = 100
			var node core.NodeId = 777
			stream := config.GetIdFromName("RO")
			var chunks []core.Chunk
			chunks = append(chunks, makeChunks(config, stream, node, 5, 95)...)
			chunks = append(chunks, makeChunks(config, stream, node, 100, 100)...)
			chunks = append(chunks, makeChunks(config, stream, node, 200, 100)...)
			c := cmwc.MakeGoodCmwc()
			c.Seed(123)
			rng := rand.New(c)
			// shuffle
			for i := range chunks {
				swap := rng.Intn(len(chunks)-i) + i
				chunks[i], chunks[swap] = chunks[swap], chunks[i]
			}

			// decoy is used to sync up our test with the some routines in sluice
			decoy := core.Chunk{Stream: config.GetIdFromName("UU")}
			goldenSt := core.MakeSequenceTracker(stream, node, 5)
			for len(chunks) > 0 {
				// Send 150 chunks
				for i := 0; i < 150 && len(chunks) > 0; i++ {
					select {
					case fromHost <- chunks[0]:
						goldenSt.AddSequenceId(chunks[0].Sequence)
						chunks = chunks[1:]
					case <-toHost:
					}
				}

				// The first bach of confirmation packets might not be up-to-date, so take those
				// packets until we're able to send the decoy, then take and store the next set of
				// confirmation packets.
				var confirmationChunks []core.Chunk
				decoyCount := 0
				<-toHost
				for decoyCount < 2 {
					select {
					case confirm := <-toHost:
						if decoyCount > 0 {
							confirmationChunks = append(confirmationChunks, confirm)
						}
					case fromHost <- decoy:
						confirmationChunks = append(confirmationChunks, <-toHost)
						decoyCount++
					}
				}
				// We want it to be greater than 1 because we want to test that independent chunks
				// can be combined to get all of the information we want.
				So(len(confirmationChunks), ShouldBeGreaterThan, 1)

				var sts []*core.SequenceTracker
				for _, chunk := range confirmationChunks {
					st, err := core.ParseSequenceTrackerChunkData(chunk.Data)
					So(err, ShouldBeNil)
					So(st, ShouldNotBeNil)
					if st.StreamId() == stream && st.NodeId() == node {
						sts = append(sts, st)
					}
				}
				So(len(sts), ShouldBeGreaterThanOrEqualTo, 1)

				coverage := make(map[core.SequenceId]bool)
				goldCoverage := make(map[core.SequenceId]bool)
				for _, st := range sts {
					// Don't need to check MaxContiguous, all we really care about is whether
					// Contains() is always correct.
					for sequence := core.SequenceId(0); sequence < 500; sequence++ {
						if st.Contains(sequence) {
							coverage[sequence] = true
						}
						if goldenSt.Contains(sequence) {
							goldCoverage[sequence] = true
						}
					}
				}
				So(coverage, ShouldResemble, goldCoverage)
			}
		})

	})
}
