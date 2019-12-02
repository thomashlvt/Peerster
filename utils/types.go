package utils

import (
	"fmt"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/constants"
)

// Definition of all message types

type Message struct {
	Text        string
	Destination *string
	File        *string
	Request     *[]byte
	Keywords    *[]string
	Budget      *uint64
}

type SimpleMessage struct {
	OriginalName  string
	RelayPeerAddr string
	Contents      string
}

type RumorMessage struct {
	Origin string
	ID     uint32
	Text   string
}

type PrivateMessage struct {
	Origin      string
	ID          uint32
	Text        string
	Destination string
	HopLimit    uint32
}

type PeerStatus struct {
	Identifier string
	NextID     uint32
}

type StatusPacket struct {
	Want []PeerStatus
}

type GossipPacket struct {
	Simple *SimpleMessage
	Rumor *RumorMessage
	Status *StatusPacket
	Private *PrivateMessage
	DataRequest *DataRequest
	DataReply *DataReply
	SearchRequest *SearchRequest
	SearchReply *SearchReply
}

type AddrGossipPacket struct {
	Address UDPAddr
	Gossip  *GossipPacket
}

type Messages struct {
	Msgs []*RumorMessage
}

type DataRequest struct {
	Origin      string
	Destination string
	HopLimit    uint32
	HashValue   []byte
}

type DataReply struct {
	Origin      string
	Destination string
	HopLimit    uint32
	HashValue   []byte
	Data        []byte
}

type SearchRequest struct {
	Origin string
	Budget uint64
	Keywords []string
}

type SearchReply struct {
	Origin string
	Destination string
	HopLimit uint32
	Results []*SearchResult
}

type SearchResult struct {
	FileName string
	MetafileHash []byte
	ChunkMap []uint64
	ChunkCount uint64
}

// Messages that can be directly sent from peer to peer:
// PrivateMessages, DataRequest and DataReply
type PointToPointMessage interface {
	GetOrigin() string
	GetDestination() string
	HopIsZero() bool
	DecrHopLimit()

	ToGossip() *GossipPacket
}

// Implement the point to point interface for PrivateMessage
func (p *PrivateMessage) GetOrigin() string       { return p.Origin }
func (p *PrivateMessage) GetDestination() string  { return p.Destination }
func (p *PrivateMessage) HopIsZero() bool         { return p.HopLimit == 0 }
func (p *PrivateMessage) DecrHopLimit()           { p.HopLimit -= 1 }
func (p *PrivateMessage) ToGossip() *GossipPacket { return &GossipPacket{Private: p} }

// Implement the point to point interface for DataRequest
func (d *DataRequest) GetOrigin() string       { return d.Origin }
func (d *DataRequest) GetDestination() string  { return d.Destination }
func (d *DataRequest) HopIsZero() bool         { return d.HopLimit == 0 }
func (d *DataRequest) DecrHopLimit()           { d.HopLimit -= 1 }
func (d *DataRequest) ToGossip() *GossipPacket { return &GossipPacket{DataRequest: d} }

// Implement the point to point interface for DataReply
func (d *DataReply) GetOrigin() string       { return d.Origin }
func (d *DataReply) GetDestination() string  { return d.Destination }
func (d *DataReply) HopIsZero() bool         { return d.HopLimit == 0 }
func (d *DataReply) DecrHopLimit()           { d.HopLimit -= 1 }
func (d *DataReply) ToGossip() *GossipPacket { return &GossipPacket{DataReply: d} }

// Implement the point to point interface for SearchReply
func (s *SearchReply) GetOrigin() string       { return s.Origin }
func (s *SearchReply) GetDestination() string  { return s.Destination }
func (s *SearchReply) HopIsZero() bool         { return s.HopLimit == 0 }
func (s *SearchReply) DecrHopLimit()           { s.HopLimit -= 1 }
func (s *SearchReply) ToGossip() *GossipPacket { return &GossipPacket{SearchReply: s} }

// Get point to point message from GossipPacket
func (g *GossipPacket) ToP2PMessage() PointToPointMessage {
	if g.Private != nil {
		return g.Private
	} else if g.DataRequest != nil {
		return g.DataRequest
	} else if g.DataReply != nil {
		return g.DataReply
	} else if g.SearchReply != nil {
		return g.SearchReply
	} else {
		return nil
	}
}

// Helper function to convert []byte hashes to [32]byte hashes
func To32Byte(bs []byte) [32]byte {
	if len(bs) != 32 {
		if Debug {
			fmt.Println("[DEBUG] Warning: To32Byte is transforming byte slice with len != 32")
		}
	}
	var hash [32]byte
	copy(hash[:], bs[:32])
	return hash
}
