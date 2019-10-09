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
	name string
	peers *Set

	ID uint32

	in chan *Packet
	out chan *Packet
	uiIn chan *Packet
	uiOut chan *Packet

	state map[string]uint32
	stateMutex *sync.RWMutex

	messages map[string] []*RumorMessage
	messagesMutex *sync.RWMutex

	statusChans map [UDPAddr] chan *StatusPacket
	statusChansMutex *sync.RWMutex

	timeout time.Duration
	maxRetries int
	antiEntropyTimout time.Duration

	debug bool
}

func NewRumorer(name string, peers *Set,
	in chan *Packet, out chan *Packet, uiIn chan *Packet, uiOut chan *Packet, debug bool) *Rumorer {

	return &Rumorer{
		name:        name,
		peers:       peers,
		ID:          1,
		in:          in,
		out:         out,
		uiIn:        uiIn,
		uiOut:       uiOut,
		state:       make(map[string]uint32),
		stateMutex:  &sync.RWMutex{},
		messages:    make(map[string][]*RumorMessage),
		messagesMutex: &sync.RWMutex{},
		statusChans: make(map[UDPAddr] chan *StatusPacket),
		statusChansMutex: &sync.RWMutex{},
		timeout:     time.Second * 30,
		antiEntropyTimout: time.Second * 10,
		debug:       debug,
		maxRetries:  4,
	}
}

func (r *Rumorer) Name() string {
	return r.name
}

func (r *Rumorer) GetMessages() []*RumorMessage {
	res := make([]*RumorMessage, 0)
	for _, v := range r.messages {
		res = append(res, v...)
	}
	return res
}

func (r *Rumorer) GetPeers() []UDPAddr {
	return r.peers.Data()
}

func (r *Rumorer) AddPeer(peer UDPAddr) {
	r.peers.Add(peer)
}

func (r *Rumorer) Run() {
	go func() {
		for {
			pack := <- r.uiIn
			fmt.Printf("PEERS %v\n", r.peers)
			go r.uiIngress(pack.Data, pack.Addr)
		}
	}()

	go func() {
		for {
			pack := <- r.in
			fmt.Printf("PEERS %v\n", r.peers)
			go r.gossipIngress(pack.Data, pack.Addr)
		}
	}()

	go func() {
		for {
			go r.antiEntropy()

			timer := time.NewTicker(r.antiEntropyTimout)
			<- timer.C
		}
	}()
}

func (r *Rumorer) antiEntropy() {
	if r.debug {
		fmt.Printf("[DEBUG] running antientropy\n")
	}
	randPeer, ok := r.peers.Rand()
	if !ok {
		fmt.Printf("[DEBUG] could not select random peer\n")
		return
	}
	r.sendState(randPeer)
}

func (r *Rumorer) uiIngress(data []byte, address UDPAddr) {
	msg := ClientMessage{}
	err := protobuf.Decode(data, &msg)
	if err != nil {
		panic(fmt.Sprintf("ERROR when decoding packet: %v", err))
	}

	if msg.POST != nil {
		fmt.Printf("CLIENT MESSAGE %s\n", *msg.POST)
		fmt.Printf("PEERS %s\n", r.peers)

		rumor := &RumorMessage{
			Origin: r.name,
			ID:     r.ID,
			Text:   *msg.POST,
		}
		r.ID += 1
		r.handleRumor(rumor, UDPAddr{})
	} else if msg.GET != nil {
		var msgs []*RumorMessage
		r.messagesMutex.RLock()
		for _, msg := range r.messages {
			msgs = append(msgs, msg...)
		}
		r.messagesMutex.RUnlock()
		bytes, err := protobuf.Encode(&Messages{Msgs:msgs})
		if err != nil {
			panic(fmt.Sprintf("ERROR could not encode packet: %v", err))
		}

		r.uiOut <- &Packet{address, bytes}
	}
}

func (r *Rumorer) gossipIngress(data []byte, address UDPAddr) {
	packet := GossipPacket{}
	err := protobuf.Decode(data, &packet)
	if err != nil {
		panic(fmt.Sprintf("ERROR when decoding packet: %v", err))
	}

	// Dispatch packet according to type
	if packet.Rumor != nil {
		r.handleRumor(packet.Rumor, address)
	} else if packet.Status != nil {
		r.handleStatus(packet.Status, address)
	} // Ignore others
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
			case <- interrupted:
				return
			case status := <- statusChan:
				for _, entry := range status.Want {
					if entry.Identifier >= msg.Origin && entry.NextID == msg.ID + 1 {
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
		return nil
	case status := <-ack:
		if r.debug {
			fmt.Printf("[DEBUG] Packet confirmed\n")
		}
		return status
	}
}

func (r *Rumorer) startCoinMongering(msg *RumorMessage, except UDPAddr) {
	if rand.Int() % 2 == 0 {
		randPeer, ok := r.peers.RandExcept(except)
		if !ok {
			fmt.Printf("[DEBUG] could not select random peer\n")
			return
		}
		fmt.Printf("FLIPPED COIN sending rumor to %v\n", randPeer)
		r.startMongering(msg, randPeer)
	}
}

func (r *Rumorer) startMongering(msg *RumorMessage, with UDPAddr) bool {
	// Set "connection" to peer by listening on status and rumor channels
	r.statusChansMutex.Lock()
	r.statusChans[with] = make(chan *StatusPacket)
	r.statusChansMutex.Unlock()

	fmt.Printf("MONGERING with %v\n", with)
	status := r.sendRumorWait(msg, with, r.statusChans[with])
	if status == nil {
		return false
	}

	iHave, youHave := r.compareStatus(status)
	if iHave != nil {
		r.messagesMutex.RLock()
		toSend := r.messages[iHave.Identifier][iHave.NextID]
		r.messagesMutex.RUnlock()

		r.startMongering(toSend, with) //TODO retries?
	} else if youHave != nil {
		r.sendState(with) //TODO retries?
	} else {
		fmt.Printf("IN SYNC WITTH %v\n", with)
		r.startCoinMongering(msg, with) //TODO retries?
	}

	r.statusChansMutex.Lock()
	delete(r.statusChans, with)
	r.statusChansMutex.Unlock()

	return true
}

func (r *Rumorer) compareStatus(msg *StatusPacket) (iHave *PeerStatus, youHave*PeerStatus) {
	r.stateMutex.RLock()
	defer r.stateMutex.RUnlock()

	iHave, youHave = nil, nil

	origins := make(map[string]bool)

	for _, entry := range msg.Want {
		origins[entry.Identifier] = true

		id, exists := r.state[entry.Identifier]
		if !exists {
			id = 0
		}
		if id > entry.NextID {
			iHave = &PeerStatus{
				Identifier: entry.Identifier,
				NextID:     entry.NextID,
			}
			if r.debug {
				fmt.Printf("[DEBUG] CompareStatus: I AM IN FRONT\n")
			}
			return
		} else if id < entry.NextID {
			// HE IS IN FRONT
			youHave = &PeerStatus{
				Identifier: entry.Identifier,
				NextID:     id,
			}
			if r.debug {
				fmt.Printf("[DEBUG] CompareStatus: HE IS IN FRONT\n")
			}
			return
		} else {
			if r.debug {
				fmt.Printf("[DEBUG] CompareStatus: WE'RE GUCCI\n")
			}
		}
	}
	// origins that the peer does not now about: send the message with ID 0
	for origin, _ := range r.state {
		if !origins[origin] {
			iHave = &PeerStatus{
				Identifier: origin,
				NextID:     0,
			}
			if r.debug {
				fmt.Printf("[DEBUG] CompareStatus: I AM IN FRONT\n")
			}
			return
		}
	}
	return
}

func (r *Rumorer) handleRumor(msg *RumorMessage, sender UDPAddr) {
	accepted := r.updateState(msg)
	if sender.String() != "" { // TODO `&& accepted` ?
		// expand peers list
		r.peers.Add(sender)

		r.sendState(sender)

		fmt.Printf("RUMOR origin %v from %v ID %v contents %v\n", msg.Origin, sender, msg.ID, msg.Text)
	}
	if accepted {
		if r.debug {
			fmt.Printf("[DEBUG] RUMOR accepted\n")
		}
		ok, retries := false, 0
		if r.peers.Len() == 1 && sender.String() != "" {
			// No rumoring needed when message comes from only other peer
			return
		}
		for !ok || retries == r.maxRetries {
			randPeer, okRand := r.peers.RandExcept(sender)
			if !okRand {
				fmt.Printf("[DEBUG] could not select random peer\n")
				return
			}
			ok = r.startMongering(msg, randPeer)
			retries += 1
		}
	}
}

func (r *Rumorer) handleStatus(msg *StatusPacket, sender UDPAddr) {
	fmt.Printf("STATUS from %v ", sender)
	for _, entry := range msg.Want {
		fmt.Printf("peer %v nextID %v ", entry.Identifier, entry.NextID)
	}
	fmt.Printf("\n")
	// expand peers list
	r.peers.Add(sender)

	r.statusChansMutex.RLock()
	c, exists := r.statusChans[sender]
	r.statusChansMutex.RUnlock()

	if exists {
		if r.debug {
			fmt.Printf("[DEBUG] Sending status from %v to mongering process\n", sender)
		}
		// Let mongering process handle status
		c <- msg
	} else {
		// Handle status independently of running mongering process
		iHave, youHave := r.compareStatus(msg)
		if iHave != nil {
			r.messagesMutex.RLock()
			toSend := r.messages[iHave.Identifier][iHave.NextID]
			r.messagesMutex.RUnlock()

			r.startMongering(toSend, sender)
		} else if youHave != nil {
			r.sendState(sender)
		}
	}
}


func (r *Rumorer) send(packet *GossipPacket, addr UDPAddr) {
	bytes, err := protobuf.Encode(packet)
	if err != nil {
		panic(fmt.Sprintf("ERROR could not encode packet: %v", err))
	}

	r.out <- &Packet{addr, bytes}
}

func (r *Rumorer) sendState(addr UDPAddr) {
	r.stateMutex.RLock()
	defer r.stateMutex.RUnlock()

	if r.debug {
		fmt.Printf("[DEBUG] sendState: %v to %v\n", r.state, addr.String())
	}

	var ack []PeerStatus
	for origin, next := range r.state {
		ack = append(ack, PeerStatus{origin, next})
	}

	r.send(&GossipPacket{Status: &StatusPacket{Want: ack}}, addr)
}

func (r *Rumorer) updateState(msg *RumorMessage) (res bool) {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()

	curr, exists := r.state[msg.Origin]
	if !exists {
		curr = 1
	}
	if curr == msg.ID {
		r.state[msg.Origin] = curr + 1
		r.messagesMutex.Lock()
		r.messages[msg.Origin] = append(r.messages[msg.Origin], msg)
		r.messagesMutex.Unlock()
		res = true
	} else {
		res = false
	}
	return res
}
