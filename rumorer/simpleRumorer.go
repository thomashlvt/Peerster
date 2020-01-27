package rumorer

import (
	. "github.com/thomashlvt/Peerster/confirmationRumorer"
	. "github.com/thomashlvt/Peerster/constants"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	"sync"

	"fmt"
)

type SimpleRumorer struct {
	addr          string
	name          string
	peers         *Set
	messages      []*RumorMessage
	messagesMutex sync.RWMutex

	// The rumorer communicates through these channels
	in   chan *AddrGossipPacket
	out  chan *AddrGossipPacket
	uiIn chan *Message
}

func NewSimpleRumorer(addr string, name string, peers *Set, in chan *AddrGossipPacket, out chan *AddrGossipPacket, uiIn chan *Message) *SimpleRumorer {
	return &SimpleRumorer{
		addr:          addr,
		name:          name,
		peers:         peers,
		messages:      make([]*RumorMessage, 0),
		messagesMutex: sync.RWMutex{},
		in:            in,
		out:           out,
		uiIn:          uiIn,
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

func (s *SimpleRumorer) UIIn() chan *Message {
	return s.uiIn
}

func (s *SimpleRumorer) TLCIn() chan *TLCMessageWithReplyChan {
	// Simply for compliance with the interface: this does not work in simple mode
	return nil
}

func (s *SimpleRumorer) TLCOut() chan *TLCMessage {
	// Simply for compliance with the interface: this does not work in simple mode
	return nil
}

func (s *SimpleRumorer) Run() {
	go func() {
		for {
			// Wait for packets on the incoming communication channel
			pack := <-s.in

			// Decode the packet
			if pack.Gossip.Simple != nil {
				go s.handleSimpleMSg(pack.Gossip.Simple, pack.Address)
			} // ignore other packets (RumorMessage and StatusPacket)
		}
	}()

	go func() {
		for {
			msg := <-s.uiIn
			go s.handleClientMsg(msg)
		}
	}()
}

func (s *SimpleRumorer) handleSimpleMSg(msg *SimpleMessage, addr UDPAddr) {
	s.messagesMutex.Lock()

	// Store relay in set of known peers
	s.peers.Add(UDPAddr{Addr: msg.RelayPeerAddr})

	if HW1 {
		fmt.Printf("SIMPLE MESSAGE origin %v from %v contents %v\n",
			msg.OriginalName, msg.RelayPeerAddr, msg.Contents)
		fmt.Printf("PEERS %s\n", s.peers)
	}

	// Save message
	s.messages = append(s.messages, &RumorMessage{
		Origin: msg.OriginalName,
		ID:     uint32(len(s.messages) + 1), // for the GUI to see the messages as unique
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
	if HW1 {
		fmt.Printf("CLIENT MESSAGE %s\n", msg.Text)
		fmt.Printf("PEERS %s\n", s.peers)
	}

	// Save message
	s.messages = append(s.messages, &RumorMessage{
		Origin: s.addr,
		ID:     uint32(len(s.messages) + 1), // for the GUI to see the messages as unique
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
	// Send it into the outgoing communication channel
	s.out <- &AddrGossipPacket{addr, gossip}
}
