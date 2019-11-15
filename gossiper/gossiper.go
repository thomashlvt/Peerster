package gossiper

import (
	. "github.com/thomashlvt/Peerster/files"
	. "github.com/thomashlvt/Peerster/privateRumorer"
	. "github.com/thomashlvt/Peerster/rumorer"
	. "github.com/thomashlvt/Peerster/utils"
	. "github.com/thomashlvt/Peerster/web"
)

type Gossiper struct {
	Dispatcher *Dispatcher

	WebServer *WebServer

	Rumorer GenericRumorer

	PrivateRumorer *PrivateRumorer

	FileHandler *FileHandler

	name string

	// Whether to print debug information
	debug bool
}

func NewGossiper(name string, peers *Set, simple bool, uiPort string, gossipAddr string,
	debug bool, antiEntropy int, routeRumoringTimeout int, hw1 bool, hw2 bool) *Gossiper {
	// Create the dispatcher
	disp := NewDispatcher(uiPort, gossipAddr)

	// Create the simple/normal rumorer
	var rumorer GenericRumorer
	if simple {
		rumorer = NewSimpleRumorer(gossipAddr, name, peers, disp.RumorerGossipIn, disp.RumorerOut, disp.RumorerUIIn, debug, hw1, hw2)
	} else {
		rumorer = NewRumorer(name, peers, disp.RumorerGossipIn, disp.RumorerOut, disp.RumorerUIIn, debug, antiEntropy, hw1, hw2)
	}

	// Create the rumorer for private messages
	privateRumorer := NewPrivateRumorer(name, disp.PrivateRumorerGossipIn, disp.PrivateRumorerUIIn,
		disp.PrivateRumorerGossipOut, disp.RumorerUIIn, disp.FileHandlerIn, routeRumoringTimeout, gossipAddr, debug, hw1, hw2)

	fileHandler := NewFileHandler(name, disp.FileHandlerIn, disp.FileHandlerUIIn, disp.PrivateRumorerGossipIn, debug, hw1, hw2)

	// Create the webserver for interacting with the rumorer
	webServer := NewWebServer(rumorer, privateRumorer, fileHandler, uiPort)

	return &Gossiper{
		Dispatcher:     disp,
		WebServer:      webServer,
		Rumorer:        rumorer,
		PrivateRumorer: privateRumorer,
		FileHandler:    fileHandler,
		name:           name,
		debug:          debug,
	}
}

func (g *Gossiper) Run() {
	g.Dispatcher.Run()
	g.Rumorer.Run()
	g.PrivateRumorer.Run()
	g.FileHandler.Run()
	if g.WebServer != nil {
		g.WebServer.Run()
	}
}
