package privateRumorer

import (
	"fmt"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	"sync"
	"time"
)

const hopLimit uint32 = 10 // Default hopLimit

type PrivateRumorer struct {
	routingTable *RoutingTable

	messages      map[string][]*PrivateMessage
	messagesMutex *sync.RWMutex

	in      chan *AddrGossipPacket
	out     chan *AddrGossipPacket
	uiIn    chan *Message
	fileOut chan *AddrGossipPacket

	rumorerUIIn chan *Message // Channel on which the 'public' rumorer listens for UI messages: used to spread a route rumor

	name  string

	routeRumoringTimeout time.Duration
	hopLimit             uint32

	debug bool
	hw1   bool
	hw2   bool
}

func NewPrivateRumorer(name string, in chan *AddrGossipPacket, uiIn chan *Message,
	out chan *AddrGossipPacket, rumorerUIIn chan *Message, fileOut chan *AddrGossipPacket, routeRumoringTimeout int,
	gossipAddr string, debug bool, hw1 bool, hw2 bool) *PrivateRumorer {

	routingTable := NewRoutingTable()
	routingTable.Add(name, UDPAddr{gossipAddr}, 0, false) // Add ourselves to the routing table

	return &PrivateRumorer{
		routingTable:         routingTable,
		messages:             make(map[string][]*PrivateMessage),
		messagesMutex:        &sync.RWMutex{},
		in:                   in,
		out:                  out,
		uiIn:                 uiIn,
		rumorerUIIn:          rumorerUIIn,
		fileOut:              fileOut,
		name:                 name,
		routeRumoringTimeout: time.Duration(routeRumoringTimeout) * time.Second,
		hopLimit:             hopLimit,
		debug:                debug,
		hw1:                  hw1,
		hw2:                  hw2,
	}
}

func (pr *PrivateRumorer) Origins() []string {
	return pr.routingTable.Origins()
}

func (pr *PrivateRumorer) PrivateMessages(origin string) []string {
	pr.messagesMutex.RLock()
	defer pr.messagesMutex.RUnlock()
	res := make([]string, len(pr.messages[origin]))
	for i, msg := range pr.messages[origin] {
		res[i] = msg.Text
	}
	return res
}

func (pr *PrivateRumorer) UIIn() chan *Message {
	return pr.uiIn
}

func (pr *PrivateRumorer) routeRumoring() {
	for {
		pr.rumorerUIIn <- &Message{Text: ""}

		timer := time.NewTicker(pr.routeRumoringTimeout)
		<-timer.C
	}
}

func (pr *PrivateRumorer) Run() {
	// if rtimer specified: send route rumor periodically
	if pr.routeRumoringTimeout != 0 {
		go pr.routeRumoring()
	}

	go func() {
		for {
			select {
			case packet := <-pr.in:
				go func() {
					if packet.Gossip.Rumor != nil {
						pr.handleRumor(packet.Gossip.Rumor, packet.Address)
					}
					p2pMsg := packet.Gossip.ToP2PMessage()
					if p2pMsg != nil {
						if pr.debug {
							fmt.Printf("[DEBUG] Received P2P Message from %s to %s\n", p2pMsg.GetOrigin(), p2pMsg.GetDestination())
						}
						pr.handlePointToPointMessage(p2pMsg, packet.Address)
					}
				}()

			case msg := <-pr.uiIn:
				go pr.handleUIMessage(msg)
			}
		}
	}()
}

func (pr *PrivateRumorer) handleRumor(rumor *RumorMessage, addr UDPAddr) {
	printDSDV := rumor.Text != ""
	pr.routingTable.Add(rumor.Origin, addr, rumor.ID, printDSDV)
}

func (pr *PrivateRumorer) handlePointToPointMessage(msg PointToPointMessage, addr UDPAddr) {
	gossip := msg.ToGossip()
	if msg.GetDestination() == pr.name {
		// Message reached destination
		if gossip.Private != nil {
			if pr.debug {
				fmt.Printf("[DEBUG] Saving Private Message from %s\n", msg.GetOrigin())
			}
			pr.savePrivateMessage(gossip.Private)
		} else if gossip.DataReply != nil || gossip.DataRequest != nil {
			if pr.debug {
				fmt.Printf("[DEBUG] Let File handling handle Data Reply/Request from %s\n", msg.GetOrigin())
			}
			pr.fileOut <- &AddrGossipPacket{addr, gossip}
		}
		return
	} else {
		if pr.debug {
			fmt.Printf("[DEBUG] %v != %v\n", msg.GetDestination(), pr.name)
		}
	}

	if msg.HopIsZero() {
		if pr.debug {
			fmt.Printf("[DEBUG] Dropped message from %v to %v\n", msg.GetOrigin(), msg.GetDestination())
		}
		return // Discard the message
	}

	// Message is not for us, and can be sent further: send it

	// Lower hopLimit
	msg.DecrHopLimit()

	sendTo, found := pr.routingTable.Get(msg.GetDestination())
	if found {
		if pr.debug {
			fmt.Printf("[DEBUG] Sending P2P from %s to %s via %s\n", msg.GetOrigin(), msg.GetDestination(), sendTo)
		}
		pr.out <- &AddrGossipPacket{sendTo, gossip}
	} else {
		if pr.debug {
			fmt.Printf("[DEBUG] UNKNOWN DESTINATION: %s\n", msg.GetDestination())
		}
	}
}

func (pr *PrivateRumorer) handleUIMessage(msg *Message) {
	fmt.Printf("CLIENT MESSAGE %v dest %v\n", msg.Text, *msg.Destination)

	// Send the message received from the client
	gossip := GossipPacket{Private: &PrivateMessage{
		Origin:      pr.name,
		ID:          0,
		Text:        msg.Text,
		Destination: *msg.Destination,
		HopLimit:    pr.hopLimit - 1, // All peers on the path (including the source) have to decrement the hop limit
	}}

	sendTo, found := pr.routingTable.Get(*msg.Destination)
	if found {
		pr.out <- &AddrGossipPacket{sendTo, &gossip}
	} else {
		if pr.debug {
			fmt.Printf("[DEBUG] UNKNOWN DESTINATION: %s\n", *msg.Destination)
		}
	}
}


func (pr *PrivateRumorer) savePrivateMessage(msg *PrivateMessage) {
	// Ensure thread-safe access
	pr.messagesMutex.Lock()
	defer pr.messagesMutex.Unlock()

	// Destination reached
	fmt.Printf("PRIVATE origin %s hop-limit %d contents %s\n", msg.Origin, msg.HopLimit, msg.Text)

	if _, ok := pr.messages[msg.Origin]; !ok {
		pr.messages[msg.Origin] = make([]*PrivateMessage, 0)
	}
	pr.messages[msg.Origin] = append(pr.messages[msg.Origin], msg)
}