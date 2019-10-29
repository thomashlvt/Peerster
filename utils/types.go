package utils

import . "github.com/thomashlvt/Peerster/udp"

// Definition of all message types

type Message struct {
	Text string
	Destination *string
	File *string
	Request *[]byte
}

type SimpleMessage struct {
	OriginalName string
	RelayPeerAddr string
	Contents string
}

type RumorMessage struct {
	Origin string
	ID uint32
	Text string
}

type PrivateMessage struct {
	Origin string
	ID uint32
	Text string
	Destination string
	HopLimit uint32
}

type PeerStatus struct {
	Identifier string
	NextID uint32
}

type StatusPacket struct {
	Want []PeerStatus
}

type GossipPacket struct {
	Simple *SimpleMessage
	Rumor *RumorMessage
	Status *StatusPacket
	Private *PrivateMessage
}

type AddrGossipPacket struct {
	Address UDPAddr
	Gossip *GossipPacket
}

type Messages struct {
	Msgs []*RumorMessage
}
