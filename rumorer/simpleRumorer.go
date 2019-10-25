package rumorer

import (
	"github.com/dedis/protobuf"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	"sync"

	"fmt"
)

type SimpleRumorer struct {
	addr     string
	name     string
	peers    *Set
	messages []*RumorMessage
	messagesMutex sync.RWMutex

	// The rumorer communicates through these channels
	in   chan *Packet
	out  chan *Packet
	uiIn chan *Packet

	debug bool
}

func NewSimpleRumorer(addr string, name string, peers *Set, in chan *Packet, out chan *Packet, uiIn chan *Packet, debug bool) *SimpleRumorer {
	return &SimpleRumorer{
		addr:     addr,
		name:     name,
		peers:    peers,
		messages: make([]*RumorMessage, 0),
		messagesMutex: sync.RWMutex{},
		in:       in,
		out:      out,
		uiIn:     uiIn,
		debug:    debug,
	}
}

func (s *SimpleRumorer) Name() string {
	return s.name
}

func (s *SimpleRumorer) Messages() []*RumorMessage {
	s.messagesMutex.RLock()
	defer s.messagesMutex.RUnlock()
	return s.messages
}

func (s *SimpleRumorer) Peers() []UDPAddr {
	return s.peers.Data()
}

func (s *SimpleRumorer) AddPeer(peer UDPAddr) {
	s.peers.Add(peer)
}

func (s *SimpleRumorer) Run() {
	go func() {
		for {
			// Wait for packets on the incoming communication channel
			pack := <-s.in

			// Decode the packet
			gossipPack := GossipPacket{}
			err := protobuf.Decode(pack.Data, &gossipPack)
			if err != nil {
				panic(fmt.Sprintf("ERROR could not decode packet: %v", err))
			}

			if gossipPack.Simple != nil {
				go s.handleSimpleMSg(gossipPack.Simple, pack.Addr)
			} // ignore other packets (RumorMessage and StatusPacket)
		}
	}()

	go func() {
		for {
			pack := <-s.uiIn
			msg := Message{}
			err := protobuf.Decode(pack.Data, &msg)
			if err != nil {
				panic(fmt.Sprintf("ERROR could not decode packet: %v", err))
			}
			go s.handleClientMsg(&msg)
		}
	}()
}

func (s *SimpleRumorer) handleSimpleMSg(msg *SimpleMessage, addr UDPAddr) {
	s.messagesMutex.Lock()

	// Store relay in set of known peers
	s.peers.Add(UDPAddr{Addr: msg.RelayPeerAddr})

	fmt.Printf("SIMPLE MESSAGE origin %v from %v contents %v\n",
		msg.OriginalName, msg.RelayPeerAddr, msg.Contents)
	fmt.Printf("PEERS %s\n", s.peers)

	// Save message
	s.messages = append(s.messages, &RumorMessage{
		Origin: msg.OriginalName,
		ID:     uint32(len(s.messages)+1), // for the GUI to see the messages as unique
		Text:   msg.Contents,
	})

	s.messagesMutex.Unlock()

	// Change relay address to own address
	sender := msg.RelayPeerAddr
	msg.RelayPeerAddr = s.addr

	// Gossip message to all known peers
	for _, peer := range s.peers.Data() {
		if peer.String() != sender {
			packet := GossipPacket{Simple: msg}
			s.Send(&packet, peer)
		}
	}
}

func (s *SimpleRumorer) handleClientMsg(msg *Message) {
	s.messagesMutex.Lock()
	fmt.Printf("CLIENT MESSAGE %s\n", msg.Text)
	fmt.Printf("PEERS %s\n", s.peers)

	// Save message
	s.messages = append(s.messages, &RumorMessage{
		Origin: s.addr,
		ID:     uint32(len(s.messages)+1), // for the GUI to see the messages as unique
		Text:   msg.Text,
	})

	// Gossip message to all known peers
	packet := GossipPacket{Simple: &SimpleMessage{
		OriginalName:  s.name,
		RelayPeerAddr: s.addr,
		Contents:      msg.Text,
	}}

	s.messagesMutex.Unlock()

	for _, peer := range s.peers.Data() {
		s.Send(&packet, peer)
	}
}

func (s *SimpleRumorer) Send(gossip *GossipPacket, addr UDPAddr) {
	// Encode the message
	bytes, err := protobuf.Encode(gossip)
	if err != nil {
		panic(fmt.Sprintf("ERROR could not encode packet: %v", err))
	}

	// Send it into the outgoing communication channel
	s.out <- &Packet{addr, bytes}
}
