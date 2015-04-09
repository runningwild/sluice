# sluice
(NOTE: Sluice is in-progress.  I have implemented most of what is described here in a previous repo, but I am rewriting it for various reasons.  This README is mostly for me right now to remind me of what I need for an initial release.)

Sluice provides a simple, but powerful, layer on top of a typical host/client network topology.  It was developed with games in mind, but could be useful in a wide variety of situations.

###Features
* Multiple logical streams.  This avoids problems like TCP head-of-line blocking.
* Different levels of reliability.  Individual streams can be specified as reliable/unreliable and ordered/unordered.
* Integrity.  All packets, reliable or not, are subject to a CRC.
* Chunking. Large packets are split into chunks and reassembled on the receiving end.  This means that you can send very large packets and receive them as a single very large packet.
* Broadcasting.  Streams can broadcast, in which case all connected nodes will receive the message, or non-broadcast, in which case clients send directly to the host, and the host can send directly to individual clients.
* NAT Punchthrough.  The host collects ping time from the host to its clients, and between pairs of clients.  If the ping time from client A to client B is less than A -> Host -> B then the host may indicate that A should send any broadcast packets directly to B.  This is done by default and does not require any extra configuration.


Creating a host:
```go
config := sluice.Config{
  "PositionUpdates": sluice.StreamConfig{
    Mode: sluice.ModeUnreliableOrdered,
    Broadcast: true,
  },
  "Actions": sluice.StreamConfig{
    Mode: sluice.ModeReliableOrdered,
  },
}
host, err := sluice.MakeHost(config)
```

Creating a client:
```go
client, err := sluice.MakeClient(hostAddr)
```

###Details
Ideally you should be able to use sluice without worrying about any low level details.  If you are interested though, I'll mention some important points here.

####Overhead
To implement reliability and ordering some data needs to be added to each packet.  The data added on top of user-data is between 14 and 18 bytes per packet, depending on how many packets get merged into a single UDP packet.  This is slightly more than TCP, but also allows for some things that TCP does not.  This measurement of overhead isn't the whole story, though, since both sluice and TCP require extra communication.  When sluice is all working I'll do a comparison of overall network traffic when using sluice for traffic that is similar to TCP.

####Multiplexing
The different streams are all multiplexed onto a single UDP stream.  The stream id accounts for 2 bytes per packet, although it it might be reasonable to limit you to 256 streams, in which case that could be reduced to 1 byte per packet.

####Reliability
The client and hosts periodically send their position on each stream, if either notices that they are missing anything they send a resend request.
