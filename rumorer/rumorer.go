package rumorer

import (
	. "github.com/thomashlvt/Peerster/constants"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	"math/rand"
	"sync"

	"fmt"
	"time"
)

const ACKTIMEOUT = 2

type msgID struct {
	origin string
	id     uint32
}

type Rumorer struct {
	name  string
	peers *Set

	mongeringWith map[UDPAddr]*RumorMessage
	mongeringWithMutex *sync.RWMutex

	// ID the next message created by this peer will get
	id uint32

	// The rumorer communicates through these channels
	in   chan *AddrGossipPacket
	out  chan *AddrGossipPacket
	uiIn chan *Message

	// State of this peer, this contains the vector clock and messages
	state *State

	// Channels used to acknowledge a rumor
	ackChans      map[UDPAddr]map[msgID]chan bool
	ackChansMutex *sync.RWMutex

	// Timeout for waiting for ack for a rumor
	timeout time.Duration

	// Interval between anti-entropy runs
	antiEntropyTimout time.Duration
}

func NewRumorer(name string, peers *Set,
	in chan *AddrGossipPacket, out chan *AddrGossipPacket, uiIn chan *Message, antiEntropy int) *Rumorer {

	return &Rumorer{
		name:              name,
		peers:             peers,
		mongeringWith:     make(map[UDPAddr]*RumorMessage),
		mongeringWithMutex: &sync.RWMutex{},
		id:                1,
		in:                in,
		out:               out,
		uiIn:              uiIn,
		state:             NewState(out),
		ackChans:          make(map[UDPAddr]map[msgID]chan bool),
		ackChansMutex:     &sync.RWMutex{},
		timeout:           time.Second * ACKTIMEOUT,
		antiEntropyTimout: time.Second * time.Duration(antiEntropy),
	}
}

func (r *Rumorer) Name() string {
	return r.name
}

func (r *Rumorer) Messages() []*RumorMessage {
	return r.state.Messages()
}

func (r *Rumorer) Peers() []UDPAddr {
	return r.peers.Data()
}

func (r *Rumorer) AddPeer(peer UDPAddr) {
	r.peers.Add(peer)
}

func (r *Rumorer) runClient() {
	for {
		// Wait for and process incoming packets from the client
		msg := <-r.uiIn

		if HW1 && msg.Text != ""{
			fmt.Printf("CLIENT MESSAGE %s\n", msg.Text)
			fmt.Printf("PEERS %s\n", r.peers)
		}

		// Create a new rumor message, and pass it to the rumor handler
		rumor := &RumorMessage{
			Origin: r.name,
			ID:     r.id,
			Text:   msg.Text,
		}
		r.id += 1
		go r.handleRumor(rumor, UDPAddr{})
	}
}

func (r *Rumorer) UIIn() chan *Message {
	return r.uiIn
}

func (r *Rumorer) Run() {
	go r.runClient()
	go r.runPeer()

	if r.antiEntropyTimout != 0 {
		go r.runAntiEntropy()
	}
}

func (r *Rumorer) runPeer() {
	// Wait for and process incoming packets from other peers
	for packet := range r.in {
		go func() {
			gossip := packet.Gossip
			address := packet.Address

			// Dispatch packet according to type
			if gossip.Rumor != nil {
				msg := gossip.Rumor

				// Expand peers list
				r.peers.Add(address)

				// Print logging info
				if HW1 && msg.Text != "" {
					fmt.Printf("PEERS %v\n", r.peers)
				}

				r.handleRumor(msg, address)

			} else if gossip.Status != nil {
				msg := gossip.Status

				// Expand peers list
				r.peers.Add(address)

				// Print logging info
				if HW1 {
					toPrint := ""
					toPrint += fmt.Sprintf("STATUS from %v ", address)
					for _, entry := range msg.Want {
						toPrint += fmt.Sprintf("peer %v nextID %v ", entry.Identifier, entry.NextID)
					}
					toPrint += fmt.Sprintf("\n")
					toPrint += fmt.Sprintf("PEERS %v\n", r.peers)
					fmt.Printf(toPrint)
				}

				r.handleStatus(msg, address)
			} // Ignore SimpleMessage
		}()
	}
}

func (r *Rumorer) runAntiEntropy() {
	for {
		// Run anti-entropy every `antiEntropyTimout` seconds
		go func() {
			if Debug {
				fmt.Printf("[DEBUG] running antientropy\n")
			}
			// Send StatusPacket to a random peer
			randPeer, ok := r.peers.Rand()
			if ok {
				r.state.Send(randPeer)
			}
		}()

		timer := time.NewTicker(r.antiEntropyTimout)
		<-timer.C
	}
}

func (r *Rumorer) startMongering(msg *RumorMessage, except UDPAddr, coinFlip bool) {
	if coinFlip {
		// Flip a coin: heads -> don't start mongering
		if rand.Int()%2 == 0 {
			if Debug {
				fmt.Println("[DEBUG] FLIPPED COIN: nope")
			}
			return
		}
	}

	ok, first := false, true
	for !ok {
		// Select random peer
		randPeer, okRand := r.peers.RandExcept(except)
		if okRand {
			if coinFlip && first { // Only print FLIPPED COIN the first try, the coin was only flipped once...
				if HW1 || Debug {
					fmt.Printf("FLIPPED COIN sending rumor to %v\n", randPeer)
				}
				first = false
			}

			// Start mongering with this peer
			r.mongeringWithMutex.Lock()
			r.mongeringWith[randPeer] = msg
			if HW1 {
				fmt.Printf("MONGERING with %v\n", randPeer)
			}
			r.mongeringWithMutex.Unlock()

			ok = r.sendRumorWait(msg, randPeer)
			if !ok {
				// Not mongering with this peer so delete it from the set
				r.mongeringWithMutex.Lock()
				delete(r.mongeringWith, randPeer)
				r.mongeringWithMutex.Unlock()
			}
		} else {
			// No peers to select from: simply don't monger
			return
		}
	}
}

func (r *Rumorer) handleRumor(msg *RumorMessage, sender UDPAddr) {
	// Update peer state, and check if the message was a message we were looking for
	accepted := r.state.Update(msg)

	// If the message didn't come from the client: acknowledge the message
	if sender.String() != "" {
		r.state.Send(sender)
	}

	if accepted {
		if Debug {
			fmt.Printf("[DEBUG] RUMOR accepted\n")
		}
		// Start mongering the message
		r.startMongering(msg, sender, false) // except sender
	}
}

func (r *Rumorer) handleStatus(msg *StatusPacket, sender UDPAddr) {
	// Check if a rumor is waiting to be acknowledged
	r.ackChansMutex.RLock()
	if _, exists := r.ackChans[sender]; exists {
		for msgid, c := range r.ackChans[sender] {
			// Check if this msgid is acknowledged by msg
			for _, status := range msg.Want {
				if status.Identifier == msgid.origin && msgid.id < status.NextID {
					c <- true
				}
			}
		}
	}
	r.ackChansMutex.RUnlock()


	// Compare the received state to our state
	iHave, youHave := r.state.Compare(msg)

	if iHave != nil {
		// I have a message he wants: send this message
		toSend := r.state.Message(iHave.Identifier, iHave.NextID)
		if HW1 || HW2 {
			// We're actually not, but this is needed as output to indicate that we sent a rumor message
			fmt.Printf("MONGERING with %v\n", sender)
		}
		r.send(&GossipPacket{Rumor: toSend}, sender)

	} else if youHave != nil {
		// He has a message I need: request it by sending my state
		r.state.Send(sender)
	} else {
		// We are in sync
		if HW1 {
			fmt.Printf("IN SYNC WITH %v\n", sender)
		}
		r.mongeringWithMutex.Lock()
		if rumor, exists := r.mongeringWith[sender]; exists {
			delete(r.mongeringWith, sender)
			r.mongeringWithMutex.Unlock()
			// If we we're at one point mongering with this peer: flip a coin to start mongering again
			r.startMongering(rumor, sender, true)
		} else {
			r.mongeringWithMutex.Unlock()
		}

	}
}

func (r *Rumorer) sendRumorWait(msg *RumorMessage, to UDPAddr) bool {
	// Create ack channel
	r.ackChansMutex.Lock()
	if _, exists := r.ackChans[to]; !exists {
		r.ackChans[to] = make(map[msgID]chan bool)
	}
	ackChan := make(chan bool, 64) // TODO: vulnerable to DDOS!
	r.ackChans[to][msgID{msg.Origin, msg.ID}] = ackChan
	r.ackChansMutex.Unlock()

	// send rumor to peer
	r.send(&GossipPacket{Rumor: msg}, to)

	// start timer for timeout on ack
	timer := time.NewTicker(r.timeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		if Debug {
			fmt.Printf("[DEBUG] Timeout when waiting for status\n")
		}
		// Timed out
		// Delete ack channel
		r.ackChansMutex.Lock()
		delete(r.ackChans[to], msgID{msg.Origin, msg.ID})
		r.ackChansMutex.Unlock()
		return false

	case <-ackChan: // TODO: safer to check channel is closed!
		if Debug {
			fmt.Printf("[DEBUG] Packet confirmed\n")
		}
		// Status received
		// Delete ack channel
		r.ackChansMutex.Lock()
		delete(r.ackChans[to], msgID{msg.Origin, msg.ID})
		r.ackChansMutex.Unlock()
		return true
	}
}

func (r *Rumorer) send(packet *GossipPacket, addr UDPAddr) {
	// Send gossip packet to addr
	r.out <- &AddrGossipPacket{addr, packet}
}
