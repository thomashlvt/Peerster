package gossiper

import (
	. "github.com/thomashlvt/Peerster/privateRumorer"
	. "github.com/thomashlvt/Peerster/rumorer"
	. "github.com/thomashlvt/Peerster/utils"
	. "github.com/thomashlvt/Peerster/web"
)

type Gossiper struct {
	Dispatcher * Dispatcher

	WebServer    *WebServer

	Rumorer GenericRumorer

	PrivateRumorer *PrivateRumorer

	name string

	// Whether to print debug information
	debug bool
}

func NewGossiper(name string, peers *Set, simple bool, uiPort string, gossipAddr string,
	             debug bool, antiEntropy int, routeRumoringTimeout int) *Gossiper {
	// Create the dispatcher
	disp := NewDispatcher(uiPort, gossipAddr)

	// Both the rumorer and private rumorer get a pointer to this value
	// as they both need to access it
	// TODO: mutex for this?
	var id uint32
	id = 1

	// Create the simple/normal rumorer
	var rumorer GenericRumorer
	if simple {
		rumorer = NewSimpleRumorer(gossipAddr, name, peers, disp.RumorerGossipIn, disp.RumorerOut, disp.RumorerUIIn, debug)
	} else {
		rumorer = NewRumorer(name, peers, &id, disp.RumorerGossipIn, disp.RumorerOut, disp.RumorerUIIn, debug, antiEntropy)
	}

	// Create the rumorer for private messages
	privateRumorer := NewPrivateRumorer(name, peers, &id, disp.PrivateRumorerGossipIn, disp.PrivateRumorerUIIn,
		                                disp.PrivateRumorerGossipOut, disp.RumorerUIIn, routeRumoringTimeout, gossipAddr, debug)

	// Create the webserver for interacting with the rumorer
	webServer := NewWebServer(rumorer, privateRumorer, uiPort)

	return &Gossiper{
		Dispatcher:   disp,
		WebServer:    webServer,
		Rumorer:      rumorer,
		PrivateRumorer: privateRumorer,
		name:         name,
		debug:        debug,
	}
}

func (g *Gossiper) Run() {
	g.Dispatcher.Run()
	g.Rumorer.Run()
	g.PrivateRumorer.Run()
	if g.WebServer != nil {
		g.WebServer.Run()
	}
}
