package gossiper

import (
	"fmt"
	"github.com/dedis/protobuf"
	. "github.com/thomashlvt/Peerster/constants"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	"log"
)

type Dispatcher struct {
	// To retrieve messages that have to be dispatched to different components of the program
	// and send messages coming from the components
	UIServer     *Server
	GossipServer *Server

	// To dispatch to the 'public' rumorer
	RumorerGossipIn chan *AddrGossipPacket
	RumorerUIIn     chan *Message
	// To dispatch from the 'public' rumorer
	RumorerOut chan *AddrGossipPacket

	// To dispatch to the 'private' rumorer
	PrivateRumorerGossipIn chan *AddrGossipPacket
	PrivateRumorerUIIn     chan *Message
	// To dispatch from the 'private' rumorer
	PrivateRumorerGossipOut chan *AddrGossipPacket
	// P2P reply that are for the local node and should be handled
	// by other parts of the gossper
	PrivateRumorerLocalOut chan *AddrGossipPacket

	FileHandlerUIIn chan *Message
	FileHandlerIn   chan *AddrGossipPacket

	SearchHandlerUIIn chan *Message
	SearchHandlerIn chan *AddrGossipPacket
	SearchHandlerOut chan *AddrGossipPacket

	ConfRumorerP2PIn chan *AddrGossipPacket
}

func NewDispatcher(uiPort string, gossipAddr string) *Dispatcher {
	return &Dispatcher{
		UIServer:     NewServer("127.0.0.1:" + uiPort),
		GossipServer: NewServer(gossipAddr),

		RumorerGossipIn: make(chan *AddrGossipPacket, 1024),
		RumorerUIIn:     make(chan *Message, 1024),
		RumorerOut:      make(chan *AddrGossipPacket, 1024),

		PrivateRumorerGossipIn:  make(chan *AddrGossipPacket, 1024),
		PrivateRumorerUIIn:      make(chan *Message, 1024),
		PrivateRumorerGossipOut: make(chan *AddrGossipPacket, 1024),
		PrivateRumorerLocalOut:  make(chan *AddrGossipPacket, 1024),

		FileHandlerUIIn: make(chan *Message, 1024),
		FileHandlerIn:   make(chan *AddrGossipPacket, 1024),

		SearchHandlerUIIn: make(chan *Message, 1024),
		SearchHandlerIn: make(chan *AddrGossipPacket, 1024),
		SearchHandlerOut: make(chan *AddrGossipPacket, 1024),

		ConfRumorerP2PIn: make(chan *AddrGossipPacket, 1024),
	}
}

func (d *Dispatcher) Run() {
	d.UIServer.Run()
	d.GossipServer.Run()

	go func() {
		for pack := range d.UIServer.Ingress() {
			// Decode the packet
			msg := Message{}
			err := protobuf.Decode(pack.Data, &msg)
			if err != nil {
				panic(fmt.Sprintf("ERROR when decoding packet: %v", err))
			}

			// Dispatch client message
			d.dispatchFromClient(&msg)
		}
	}()

	go func() {
		for raw := range d.GossipServer.Ingress() {
			// Decode the packet
			packet := GossipPacket{}
			err := protobuf.Decode(raw.Data, &packet)
			if err != nil {
				panic(fmt.Sprintf("ERROR when decoding packet: %v", err))
			}

			// Dispatch gossip
			d.dispatchFromPeer(&AddrGossipPacket{raw.Addr, &packet})

		}
	}()

	go func() {
		for packet := range d.PrivateRumorerLocalOut {
			if packet.Gossip.SearchReply != nil {
				d.SearchHandlerIn <- packet
			} else if packet.Gossip.DataReply != nil || packet.Gossip.DataRequest != nil {
				d.FileHandlerIn <- packet
			} else if packet.Gossip.Ack != nil && (HW3EX2 || HW3EX3){
				d.ConfRumorerP2PIn <- packet
			}
		}
	}()

	go func() {
		for packet := range d.RumorerOut {
			bytes, err := protobuf.Encode(packet.Gossip)
			if err != nil {
				log.Fatalf("ERROR could not encode packet: %v", err)
			}
			d.GossipServer.Outgress() <- &RawPacket{packet.Address, bytes}
		}
	}()

	go func() {
		for packet := range d.PrivateRumorerGossipOut {
			bytes, err := protobuf.Encode(packet.Gossip)
			if err != nil {
				log.Fatalf("ERROR could not encode packet: %v", err)
			}
			d.GossipServer.Outgress() <- &RawPacket{packet.Address, bytes}
		}
	}()

	go func() {
		for packet := range d.SearchHandlerOut {
			bytes, err := protobuf.Encode(packet.Gossip)
			if err != nil {
				log.Fatalf("ERROR could not encode packet: %v", err)
			}
			d.GossipServer.Outgress() <- &RawPacket{packet.Address, bytes}
		}
	}()
}

func (d *Dispatcher) dispatchFromPeer(gossip *AddrGossipPacket) {
	if gossip.Gossip.Rumor != nil {
		// Make sure to print RUMOR before DSDV
		if gossip.Gossip.Rumor.Text != "" && (HW1 || HW2) {
			fmt.Printf("RUMOR origin %v from %v ID %v contents %v\n", gossip.Gossip.Rumor.Origin, gossip.Address, gossip.Gossip.Rumor.ID, gossip.Gossip.Rumor.Text)
		}

		d.PrivateRumorerGossipIn <- gossip

		d.RumorerGossipIn <- gossip

	}
	if gossip.Gossip.Status != nil || gossip.Gossip.Simple != nil {
		d.RumorerGossipIn <- gossip
	}

	if gossip.Gossip.Private != nil || gossip.Gossip.DataReply != nil || gossip.Gossip.DataRequest != nil || gossip.Gossip.Ack != nil {
		d.PrivateRumorerGossipIn <- gossip
	}

	if gossip.Gossip.SearchRequest != nil {
		d.SearchHandlerIn <- gossip
	}

	if gossip.Gossip.SearchReply != nil {
		d.PrivateRumorerGossipIn <- gossip
	}

	if gossip.Gossip.TLCMessage != nil {
		d.RumorerGossipIn <- gossip
		d.PrivateRumorerGossipIn <- gossip
	}
}

func (d *Dispatcher) dispatchFromClient(msg *Message) {
	if msg.File != nil && (msg.Request == nil || msg.Request != nil && msg.Destination != nil){
		d.FileHandlerUIIn <- msg
		return
	}

	if msg.File != nil && msg.Request != nil && msg.Destination == nil {
		d.SearchHandlerUIIn <- msg
	}

	if msg.Text != "" {
		if msg.Destination == nil {
			d.RumorerUIIn <- msg
		} else {
			d.PrivateRumorerUIIn <- msg
		}
	}

	if msg.Keywords != nil {
		d.SearchHandlerUIIn <- msg
	}

}
