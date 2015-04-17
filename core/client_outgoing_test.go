package core_test

import (
	"github.com/runningwild/clock"
	"log"
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
		incoming := make(chan core.Chunk)
		resend := make(chan core.Chunk)
		truncate := make(chan core.Chunk)
		outgoing := make(chan core.Chunk)
		handlerIsDone := make(chan struct{})
		defer func() {
			close(incoming)
			close(resend)
			close(truncate)
			for {
				select {
				case <-handlerIsDone:
					return
				case <-outgoing:
				}
			}
		}()
		go func() {
			core.ClientSendChunksHandler(config, incoming, resend, truncate, outgoing)
			close(handlerIsDone)
		}()

		Convey("Copies all incoming chunks to outgoing.", func() {
			go func() {
				incoming <- makeSimpleChunk(config.GetIdFromName("UU"), config.Node, 10)
				incoming <- makeSimpleChunk(config.GetIdFromName("UU"), config.Node, 11)
				incoming <- makeSimpleChunk(config.GetIdFromName("UU"), config.Node, 12)
				incoming <- makeSimpleChunk(config.GetIdFromName("UO"), config.Node, 20)
				incoming <- makeSimpleChunk(config.GetIdFromName("UO"), config.Node, 21)
				incoming <- makeSimpleChunk(config.GetIdFromName("UO"), config.Node, 22)
				incoming <- makeSimpleChunk(config.GetIdFromName("RU"), config.Node, 30)
				incoming <- makeSimpleChunk(config.GetIdFromName("RU"), config.Node, 31)
				incoming <- makeSimpleChunk(config.GetIdFromName("RU"), config.Node, 32)
				incoming <- makeSimpleChunk(config.GetIdFromName("RO"), config.Node, 40)
				incoming <- makeSimpleChunk(config.GetIdFromName("RO"), config.Node, 41)
				incoming <- makeSimpleChunk(config.GetIdFromName("RO"), config.Node, 42)
			}()
			pt := make(core.PacketTracker)
			for i := 0; i < 12; i++ {
				pt.Add(<-outgoing)
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
					incoming <- makeSimpleChunk(config.GetIdFromName("RU"), config.Node, core.SequenceId(i))
					incoming <- makeSimpleChunk(config.GetIdFromName("RO"), config.Node, core.SequenceId(i))
				}
			}()
			for i := 0; i < N; i++ {
				<-outgoing
				<-outgoing
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

					case chunk := <-outgoing:
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
								incoming <- makeSimpleChunk(config.GetIdFromName("RU"), config.Node, next)
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

						case chunk := <-outgoing:
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

						case chunk := <-outgoing:
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
							truncate <- core.Chunk{
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

						case chunk := <-outgoing:
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
						resend <- core.Chunk{
							Stream: core.StreamResend,
							Source: 1,
							Data:   data,
						}
					}
				}()

				pt := make(core.PacketTracker)
				for i := 0; i < N; i++ {
					pt.Add(<-outgoing)
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
						truncate <- core.Chunk{
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
							resend <- core.Chunk{
								Stream: core.StreamResend,
								Source: 1,
								Data:   data,
							}
						}
					}()
					pt := make(core.PacketTracker)
					for i := 0; i < 1; i++ {
						pt.Add(<-outgoing)
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