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

	messages map[string][]*PrivateMessage
	messagesMutex *sync.RWMutex

	in chan *AddrGossipPacket
	out chan *AddrGossipPacket
	uiIn chan *Message

	rumorerUIIn chan *Message // Channel on which the 'public' rumorer listens for UI messages: used to spread a route rumor

	peers *Set
	name string
	idPtr *uint32

	routeRumoringTimeout time.Duration
	hopLimit uint32

	debug bool
}

func NewPrivateRumorer(name string, peers *Set, idPtr *uint32, in chan *AddrGossipPacket, uiIn chan *Message,
		out chan *AddrGossipPacket, rumorerUIIn chan *Message, routeRumoringTimeout int, gossipAddr string, debug bool) *PrivateRumorer {

	routingTable := NewRoutingTable()
	routingTable.Add(name, UDPAddr{gossipAddr}, 0, false) // Add ourselves to the routing table

	return &PrivateRumorer{
		routingTable: routingTable,
		messages:     make(map[string][]*PrivateMessage),
		messagesMutex: &sync.RWMutex{},
		in:           in,
		out:          out,
		uiIn:         uiIn,
		rumorerUIIn:  rumorerUIIn,
		peers:        peers,
		name:         name,
		idPtr:        idPtr,
		routeRumoringTimeout: time.Duration(routeRumoringTimeout) * time.Second,
		hopLimit:     hopLimit,
		debug:        debug,
	}
}

func (pr *PrivateRumorer) Origins() []string {
	return pr.routingTable.Origins()
}

func (pr *PrivateRumorer) routeRumoring() {
	// Startup route rumor message
	pr.rumorerUIIn <- &Message{Text: ""}

	// if rtimer specified: send route rumor periodically
	if pr.routeRumoringTimeout != 0 {
		for {
			timer := time.NewTicker(pr.routeRumoringTimeout)
			<-timer.C

			// Send route rumor periodically
			pr.rumorerUIIn <- &Message{Text: ""}
		}
	}
}


func (pr *PrivateRumorer) Run() {
	go pr.routeRumoring()

	go func() {
		for {
			select {
			case packet := <- pr.in:
				if packet.Gossip.Rumor != nil {
					go pr.handleRumor(packet.Gossip.Rumor, packet.Address)
				} else if packet.Gossip.Private != nil {
					go pr.handlePrivateMessage(packet.Gossip.Private, packet.Address)
				}
			case msg := <- pr.uiIn:
				go pr.handleUIMessage(msg)
			}
		}
	}()
}

func (pr *PrivateRumorer) handleRumor(rumor *RumorMessage, addr UDPAddr) {
	if rumor != nil {
		printDSDV := rumor.Text != ""
		pr.routingTable.Add(rumor.Origin, addr, rumor.ID, printDSDV)
	}
}

func (pr *PrivateRumorer) savePrivateMessage(msg *PrivateMessage) {
	// Destination reached
	fmt.Printf("PRIVATE origin %s hop-limit %d contents %s", msg.Origin, msg.HopLimit, msg.Text)
	pr.messagesMutex.Lock()
	if _, ok := pr.messages[msg.Origin]; !ok {
		pr.messages[msg.Origin] = make([]*PrivateMessage, 0)
	}
	pr.messages[msg.Origin] = append(pr.messages[msg.Origin], msg)
}

func (pr *PrivateRumorer) handlePrivateMessage(msg *PrivateMessage, addr UDPAddr) {
	if msg.Destination == pr.name {
		// Message reached destination
		pr.savePrivateMessage(msg)
		return
	}

	if msg.HopLimit == 0 {
		return // Discard the message
	}

	// Lower hopLimit
	msg.HopLimit -= 1

	// Send the message
	gossip := GossipPacket{Private: msg}
	sendTo := pr.routingTable.Get(msg.Destination)
	if sendTo.Addr != "" {
		pr.out <- &AddrGossipPacket{sendTo, &gossip}
	} else {
		fmt.Printf("UNKNOWN DESTINATION: %s\n", msg.Destination)
	}
}

func (pr *PrivateRumorer) handleUIMessage(msg *Message) {
	// Send the message received from the client
	gossip := GossipPacket{Private: &PrivateMessage{
		Origin:      pr.name,
		ID:          *pr.idPtr,
		Text:        msg.Text,
		Destination: *msg.Destination,
		HopLimit:    pr.hopLimit,
	}}

	sendTo := pr.routingTable.Get(*msg.Destination)
	if sendTo.Addr != "" {
		pr.out <- &AddrGossipPacket{sendTo, &gossip}
	} else {
		fmt.Printf("UNKNOWN DESTINATION: %s\n", *msg.Destination)
	}
}


// TODO: implement sharing DSDV vectors?
// TODO: implement caching messages for couple of minutes?