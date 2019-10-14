package rumorer

import (
	"fmt"
	"github.com/dedis/protobuf"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	"sync"
)

type State struct {
	// Vector clock data structure that encodes the state
	state      map[string]uint32
	stateMutex *sync.RWMutex

	// All accepted messages, indexed by Origin
	messages      map[string]map[uint32]*RumorMessage
	messagesMutex *sync.RWMutex

	// Outgoing communication channel to send StatePackets
	out chan *Packet

	debug bool
}

func NewState(out chan *Packet, debug bool) *State {
	return &State{
		state:         make(map[string]uint32),
		stateMutex:    &sync.RWMutex{},
		messages:      make(map[string]map[uint32]*RumorMessage),
		messagesMutex: &sync.RWMutex{},
		out:           out,
		debug:         debug,
	}
}

func (s *State) Messages() []*RumorMessage {
	// Return messages as list (not map)
	res := make([]*RumorMessage, 0)
	for _, msgs := range s.messages {
		for _, v := range msgs {
			res = append(res, v)
		}
	}
	return res
}

func (s *State) Message(origin string, id uint32) (rumor *RumorMessage) {
	// Return message
	s.messagesMutex.RLock()
	rumor = s.messages[origin][id]
	s.messagesMutex.RUnlock()
	return rumor
}

func (s *State) Compare(msg *StatusPacket) (iHave *PeerStatus, youHave *PeerStatus) {
	// Returns a I have, that he doesn't have, or the other way around if I can't give him any more messages
	s.stateMutex.RLock()
	defer s.stateMutex.RUnlock()

	iHave, youHave = nil, nil

	// Keep a set of the origins the other peer knows about
	origins := make(map[string]bool)

	// Iterate over status of the other peer
	for _, entry := range msg.Want {
		// Add the origin, to the set of origins the other peer knows about
		origins[entry.Identifier] = true

		nextID, exists := s.state[entry.Identifier]
		if !exists {
			nextID = 1
		}
		if nextID > entry.NextID {
			// I am in front
			iHave = &PeerStatus{
				Identifier: entry.Identifier,
				NextID:     entry.NextID,  // This is the ID of the message I should send
			}
			if s.debug {
				fmt.Printf("[DEBUG] CompareStatus: I AM IN FRONT\n")
			}
			return
		} else if nextID < entry.NextID {
			// He is in front
			youHave = &PeerStatus{
				Identifier: entry.Identifier,
				NextID:     nextID,  // This is the ID of the message I want
			}
			if s.debug {
				fmt.Printf("[DEBUG] CompareStatus: HE IS IN FRONT\n")
			}
			return
		}
	}
	// origins that the peer does not now about: send the message with ID 1
	for origin, _ := range s.state {
		if !origins[origin] {
			iHave = &PeerStatus{
				Identifier: origin,
				NextID:     1,  // This is the ID of the message I have
			}
			if s.debug {
				fmt.Printf("[DEBUG] CompareStatus: I AM IN FRONT\n")
			}
			return
		}
	}
	return
}

func (s *State) Update(msg *RumorMessage) (res bool) {
	s.stateMutex.Lock()
	defer s.stateMutex.Unlock()

	curr, exists := s.state[msg.Origin]
	if !exists {
		curr = 1
	}
	if curr <= msg.ID {
		// Save the message
		s.messagesMutex.Lock()
		if _, ok := s.messages[msg.Origin]; !ok {
			s.messages[msg.Origin] = make(map[uint32]*RumorMessage)
		}
		s.messages[msg.Origin][msg.ID] = msg
		s.messagesMutex.Unlock()

		// Update the vector clock
		s.messagesMutex.RLock()
		_, ok := s.messages[msg.Origin][curr]
		for ok {
			curr += 1
			_, ok = s.messages[msg.Origin][curr]
			s.state[msg.Origin] = curr
		}
		s.messagesMutex.RUnlock()

		res = true
	} else {
		res = false
	}
	return res
}

func (s *State) Send(addr UDPAddr) {
	s.stateMutex.RLock()
	defer s.stateMutex.RUnlock()

	// Construct StatusPacket
	want := make([]PeerStatus, 0)
	for origin, next := range s.state {
		want = append(want, PeerStatus{origin, next})
	}

	// Encode the packet
	bytes, err := protobuf.Encode(&GossipPacket{Status: &StatusPacket{Want: want}})
	if err != nil {
		panic(fmt.Sprintf("ERROR could not encode packet: %v", err))
	}

	// Send it on the outgoing communication channel
	s.out <- &Packet{addr, bytes}
}
