package rumorer

import (
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	"math/rand"
	"sync"

	"fmt"
	"github.com/dedis/protobuf"
	"time"
)

type Rumorer struct {
	name  string
	peers *Set

	// ID the next message created by this peer will get
	ID uint32

	// The rumorer communicates through these channels
	in    chan *Packet
	out   chan *Packet
	uiIn  chan *Packet
	uiOut chan *Packet

	// State of this peer, this contains the vector clock and messages
	state *State

	// Channels used to communicate with the rumongering process
	statusChans      map[UDPAddr] chan *StatusPacket
	statusChansMutex *sync.RWMutex

	// Timeout for waiting for ack for a rumor
	timeout time.Duration

	// Interval between anti-entropy runs
	antiEntropyTimout time.Duration

	debug bool
}

func NewRumorer(name string, peers *Set,
	in chan *Packet, out chan *Packet, uiIn chan *Packet, uiOut chan *Packet, debug bool, antiEntropy int) *Rumorer {

	return &Rumorer{
		name:              name,
		peers:             peers,
		ID:                1,
		in:                in,
		out:               out,
		uiIn:              uiIn,
		uiOut:             uiOut,
		state:             NewState(out, debug),
		statusChans:       make(map[UDPAddr] chan *StatusPacket),
		statusChansMutex:  &sync.RWMutex{},
		timeout:           time.Second * 30,
		antiEntropyTimout: time.Second * time.Duration(antiEntropy),
		debug:             debug,
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

func (r *Rumorer) Run() {
	go func() {
		for {
			// Wait for and process incoming packets from the client
			pack := <-r.uiIn
			go r.uiIngress(pack.Data, pack.Addr)
		}
	}()

	go func() {
		for {
			// Wait for and process incoming packets from other peers
			pack := <-r.in
			go r.gossipIngress(pack.Data, pack.Addr)
		}
	}()

	go func() {
		for {
			// Run anti-entropy every `antiEntropyTimout` seconds
			go r.antiEntropy()

			timer := time.NewTicker(r.antiEntropyTimout)
			<-timer.C
		}
	}()
}

func (r *Rumorer) antiEntropy() {
	if r.debug {
		fmt.Printf("[DEBUG] running antientropy\n")
	}
	// Send StatusPacket to a random peer
	randPeer, ok := r.peers.Rand()
	if ok {
		r.state.Send(randPeer)
	}
}

func (r *Rumorer) uiIngress(data []byte, address UDPAddr) {
	// All messages received from the client, get processed here

	// Decode the message
	msg := ClientMessage{}
	err := protobuf.Decode(data, &msg)
	if err != nil {
		panic(fmt.Sprintf("ERROR when decoding packet: %v", err))
	}

	fmt.Printf("CLIENT MESSAGE %s\n", msg.Text)
	fmt.Printf("PEERS %s\n", r.peers)

	// Create a new rumor message, and pass it to handleRumor
	rumor := &RumorMessage{
		Origin: r.name,
		ID:     r.ID,
		Text:   msg.Text,
	}
	r.ID += 1
	r.handleRumor(rumor, UDPAddr{})
}

func (r *Rumorer) gossipIngress(data []byte, address UDPAddr) {
	// All packets received from other peers get processed here

	// Decode the packet
	packet := GossipPacket{}
	err := protobuf.Decode(data, &packet)
	if err != nil {
		panic(fmt.Sprintf("ERROR when decoding packet: %v", err))
	}

	// Dispatch packet according to type
	if packet.Rumor != nil {
		msg := packet.Rumor

		// Expand peers list
		r.peers.Add(address)

		// Print logging info
		fmt.Printf("RUMOR origin %v from %v ID %v contents %v\n", msg.Origin, address, msg.ID, msg.Text)
		fmt.Printf("PEERS %v\n", r.peers)

		r.handleRumor(msg, address)
	} else if packet.Status != nil {
		msg := packet.Status

		// Expand peers list
		r.peers.Add(address)

		// Print logging info
		fmt.Printf("STATUS from %v ", address)
		for _, entry := range msg.Want {
			fmt.Printf("peer %v nextID %v ", entry.Identifier, entry.NextID)
		}
		fmt.Printf("\n")
		fmt.Printf("PEERS %v\n", r.peers)

		r.handleStatus(msg, address)
	} // Ignore SimpleMessage
}

func (r *Rumorer) handleRumor(msg *RumorMessage, sender UDPAddr) {
	// If the message didn't come from the client: acknowledge the message
	if sender.String() != "" {
		r.state.Send(sender)
	}

	// Update peer state, and check if the message was a message we were looking for
	accepted := r.state.Update(msg)

	if accepted {
		if r.debug {
			fmt.Printf("[DEBUG] RUMOR accepted\n")
		}

		// Select a random peer to monger with
		// if this peer times out: keep retrying
		ok := false
		for !ok {
			randPeer, okRand := r.peers.RandExcept(sender)
			if okRand {
				ok = r.startMongering(msg, randPeer)
			} else {
				// No peers to select from: simply don't monger
				if r.debug {
					fmt.Printf("[DEBUG] could not select random peer\n")
				}
				return
			}
		}
	}
}

func (r *Rumorer) handleStatus(msg *StatusPacket, sender UDPAddr) {
	// Check if a mongering process is waiting for a statuspacket from sender
	// This is indicated by the presence of a status chan in statusChans
	r.statusChansMutex.RLock()
	c, exists := r.statusChans[sender]
	r.statusChansMutex.RUnlock()

	if exists {
		// There is a mongering process waiting for status packets from this peer: send it to the process
		if r.debug {
			fmt.Printf("[DEBUG] Sending status from %v to mongering process\n", sender)
		}
		c <- msg

	} else {
		// Handle status independently of running mongering process by checking
		// if the peer has any messages I need, or I have messages he needs
		iHave, youHave := r.state.Compare(msg)

		if iHave != nil {
			// I have a message he wants: start mongering
			toSend := r.state.Message(iHave.Identifier, iHave.NextID)
			r.startMongering(toSend, sender) // TODO: retry?

		} else if youHave != nil {
			// He has a message I need: request it by sending my state
			r.state.Send(sender)
		}
	}
}

func (r *Rumorer) startMongering(msg *RumorMessage, with UDPAddr) bool {
	// Returns false if peer timed out

	// Make a channel to receive status packets from the peer we are mongering with
	r.statusChansMutex.Lock()
	r.statusChans[with] = make(chan *StatusPacket)
	r.statusChansMutex.Unlock()

	fmt.Printf("MONGERING with %v\n", with)

	// Send rumor message, and wait for reply or timeout
	status := r.sendRumorWait(msg, with, r.statusChans[with])
	if status == nil {
		// Peer timed out
		return false
	}

	iHave, youHave := r.state.Compare(status)

	if iHave != nil {
		// He needs another message I have: TODO: chans!
		// get this message and continue mongering
		toSend := r.state.Message(iHave.Identifier, iHave.NextID)
		r.startMongering(toSend, with) // TODO: retry?

	} else if youHave != nil {
		// He has a message I need: ask for it by sending state
		r.state.Send(with)

	} else {
		// He and I are in sync: flip a coin to decide whether or not to continue mongering
		fmt.Printf("IN SYNC WITH %v\n", with)
		r.startCoinMongering(msg, with) // TODO retry?
	}

	// 'Unsubscribe' to status packets from the peer by deleting the channel
	r.statusChansMutex.Lock()
	delete(r.statusChans, with)
	r.statusChansMutex.Unlock()

	// Mongering didn't time out
	return true
}

func (r *Rumorer) startCoinMongering(msg *RumorMessage, except UDPAddr) {
	// Flip a coin
	if rand.Int()%2 == 0 {
		randPeer, ok := r.peers.RandExcept(except)
		if ok {
			fmt.Printf("FLIPPED COIN sending rumor to %v\n", randPeer)
			r.startMongering(msg, randPeer)
		} // if there are no other peers: simply stop mongering
	}
}

func (r *Rumorer) sendRumorWait(msg *RumorMessage, to UDPAddr, statusChan chan *StatusPacket) *StatusPacket {
	// send rumor to peer
	r.send(&GossipPacket{Rumor: msg}, to)

	// start timer for timeout on ack
	timer := time.NewTicker(r.timeout)
	defer timer.Stop()

	// channel on which the ack will be communicated by passing the status that acks the rumor
	ack := make(chan *StatusPacket)

	// channel that will be used to interrupt the goroutine below
	interrupted := make(chan bool)

	// goroutine to process the incoming statuspackets and checks them for an ack of the rumor
	// in case of ack: sends the status on ack channel
	// can be interrupted in case of timeout
	go func() {
		for {
			select {
			case <-interrupted:
				// Stop goroutine
				return
			case status := <-statusChan:
				// Check if status packet acknowledges the rumor
				for _, entry := range status.Want {
					if entry.Identifier >= msg.Origin && entry.NextID == msg.ID+1 {
						ack <- status
					}
				}
			}
		}
	}()

	select {
	case <-timer.C:
		interrupted <- true
		if r.debug {
			fmt.Printf("[DEBUG] Timeout when waiting for status\n")
		}
		// Timed out
		return nil
	case status := <-ack:
		if r.debug {
			fmt.Printf("[DEBUG] Packet confirmed\n")
		}
		// Message acknowledged
		return status
	}
}

func (r *Rumorer) send(packet *GossipPacket, addr UDPAddr) {
	// Encode and send a gossip packet
	bytes, err := protobuf.Encode(packet)
	if err != nil {
		panic(fmt.Sprintf("ERROR could not encode packet: %v", err))
	}

	r.out <- &Packet{addr, bytes}
}
