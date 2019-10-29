package gossiper

import (
	"fmt"
	"github.com/dedis/protobuf"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
)

type Dispatcher struct {
	// To retrieve messages that have to be dispatched to different components of the program
	// and send messages coming from the components
	UIServer *Server
	GossipServer *Server

	// To dispatch to the 'public' rumorer
	RumorerGossipIn chan *AddrGossipPacket
	RumorerUIIn chan *Message
	// To dispatch from the 'public' rumorer
	RumorerOut chan *AddrGossipPacket

	// To dispatch to the 'private' rumorer
	PrivateRumorerGossipIn chan *AddrGossipPacket
	PrivateRumorerUIIn chan *Message
	// To dispatch from the 'private' rumorer
	PrivateRumorerGossipOut chan *AddrGossipPacket
}

func NewDispatcher(uiPort string, gossipAddr string) *Dispatcher {
	return &Dispatcher{
		UIServer:                NewServer("127.0.0.1:" + uiPort),
		GossipServer:            NewServer(gossipAddr),
		RumorerGossipIn:         make(chan *AddrGossipPacket),
		RumorerUIIn:             make(chan *Message),
		RumorerOut:              make(chan *AddrGossipPacket),
		PrivateRumorerGossipIn:  make(chan *AddrGossipPacket),
		PrivateRumorerUIIn:      make(chan *Message),
		PrivateRumorerGossipOut: make(chan *AddrGossipPacket),
	}
}

func (d *Dispatcher) Run() {
	d.UIServer.Run()
	d.GossipServer.Run()

	go func() {
		for {
			select {
			// ------- Message coming from client -------
			case pack := <- d.UIServer.Ingress():
				// Decode the packet
				msg := Message{}
				err := protobuf.Decode(pack.Data, &msg)
				if err != nil {
					panic(fmt.Sprintf("ERROR when decoding packet: %v", err))
				}

				// Dispatch client message
				d.dispatchFromClient(&msg)
			// ------------------------------------------

			// ----- Message coming from other peer -----
			case raw := <- d.GossipServer.Ingress():
				// Decode the packet
				packet := GossipPacket{}
				err := protobuf.Decode(raw.Data, &packet)
				if err != nil {
					panic(fmt.Sprintf("ERROR when decoding packet: %v", err))
				}

				// Dispatch gossip
				d.dispatchFromPeer(&AddrGossipPacket{raw.Addr, &packet})
			//	-----------------------------------------

			// --- Messages to be sent to the outside ---
			case packet := <- d.RumorerOut:
				bytes, err := protobuf.Encode(packet.Gossip)
				if err != nil {
					panic(fmt.Sprintf("ERROR could not encode packet: %v", err))
				}
				d.GossipServer.Outgress() <- &RawPacket{packet.Address, bytes}
			// ------------------------------------------

			case packet := <- d.PrivateRumorerGossipOut:
				bytes, err := protobuf.Encode(packet.Gossip)
				if err != nil {
					panic(fmt.Sprintf("ERROR could not encode packet: %v", err))
				}
				d.GossipServer.Outgress() <- &RawPacket{packet.Address, bytes}
			}
		}
	}()
}

func (d *Dispatcher) dispatchFromPeer(gossip *AddrGossipPacket) {
	if gossip.Gossip.Rumor != nil || gossip.Gossip.Status != nil || gossip.Gossip.Simple != nil {
		d.RumorerGossipIn <- gossip

	}

	if gossip.Gossip.Rumor != nil {
		// Also send rumor messages to the private rumorer
		d.PrivateRumorerGossipIn <- gossip
	}

	if gossip.Gossip.Private != nil {
		d.PrivateRumorerGossipIn <- gossip
	}
}

func (d *Dispatcher) dispatchFromClient(msg *Message) {
	if msg.Destination == nil {
		d.RumorerUIIn <- msg
	} else {
		d.PrivateRumorerUIIn <- msg
	}

}

// TODO: copy messages sent to different destinations?