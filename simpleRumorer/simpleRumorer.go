package simpleRumorer

import (
	"github.com/dedis/protobuf"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"


	"fmt"
)

type SimpleRumorer struct {
	addr string
	name string
	peers *Set

	in chan *Packet
	out chan *Packet
	uiIn chan *Packet

	debug bool
}

func NewSimpleRumorer(addr string, name string, peers *Set, in chan *Packet, out chan *Packet, uiIn chan *Packet, debug bool) *SimpleRumorer {
	return &SimpleRumorer{
		addr:  addr,
		name:  name,
		peers: peers,
		in:    in,
		out:   out,
		uiIn:  uiIn,
		debug: debug,
	}
}

func (s *SimpleRumorer) Run() {
	go func() {
		for {
			pack := <-s.in
			gossipPack := GossipPacket{}
			err := protobuf.Decode(pack.Data, &gossipPack)
			if err != nil {
				panic(fmt.Sprintf("ERROR could not decode packet: %v", err))
			}

			if gossipPack.Simple != nil {
				s.handleSimpleMSg(gossipPack.Simple, pack.Addr)
			} // ignore other packets
		}
	}()

	go func() {
		for {
			pack := <- s.uiIn
			msg := ClientMessage{}
			err := protobuf.Decode(pack.Data, &msg)
			if err != nil {
				panic(fmt.Sprintf("ERROR could not decode packet: %v", err))
			}
			s.handleClientMsg(&msg)
		}
	}()
}

func (s *SimpleRumorer) handleSimpleMSg(msg *SimpleMessage, addr UDPAddr) {
	// Store relay in set of known peers
	s.peers.Add(UDPAddr{Addr: msg.RelayPeerAddr})

	fmt.Printf("SIMPLE MESSAGE origin %v from %v contents %v\n",
		msg.OriginalName, msg.RelayPeerAddr, msg.Contents)
	fmt.Printf("PEERS %s\n", s.peers)

	// Change relay address to own address
	msg.RelayPeerAddr = s.addr

	// gossip message to all known peers
	for _, peer := range s.peers.Data() {
		if peer.String() != msg.RelayPeerAddr {
			packet := GossipPacket{Simple: msg}
			s.Send(&packet, peer)
		}
	}
}

func (s *SimpleRumorer) handleClientMsg(msg *ClientMessage) {
	fmt.Printf("CLIENT MESSAGE %s\n", *msg.POST)
	fmt.Printf("PEERS %s\n", s.peers)

	simpleMsg := SimpleMessage{
		OriginalName: s.name,
		RelayPeerAddr: s.addr,
		Contents: *msg.POST,
	}
	packet := GossipPacket{Simple: &simpleMsg}

	// gossip message to all known peers
	for _, peer := range s.peers.Data() {
		s.Send(&packet, peer)
	}
}

func (s *SimpleRumorer) Send(gossip *GossipPacket, addr UDPAddr) {
	bytes, err := protobuf.Encode(gossip)
	if err != nil {
		panic(fmt.Sprintf("ERROR could not encode packet: %v", err))
	}
	s.out <- &Packet{addr, bytes}
}
