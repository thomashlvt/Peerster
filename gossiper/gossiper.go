package gossiper

import (
	. "github.com/thomashlvt/Peerster/rumorer"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	. "github.com/thomashlvt/Peerster/web"
)

type Gossiper struct {
	UIServer     *Server
	GossipServer *Server
	WebServer    *WebServer

	Rumorer GenericRumorer

	name string

	// Whether to print debug information
	debug bool
}

func NewGossiper(name string, peers *Set, simple bool, uiPort string, gossipAddr string, debug bool, withGUI bool, antiEntropy int) *Gossiper {
	// Networking connections
	uiServer := NewServer("127.0.0.1:" + uiPort)
	gossipServer := NewServer(gossipAddr)

	// Create the simple/normal rumorer
	var rumorer GenericRumorer
	if simple {
		rumorer = NewSimpleRumorer(gossipAddr, name, peers, gossipServer.Ingress(), gossipServer.Outgress(), uiServer.Ingress(), debug)
	} else {
		rumorer = NewRumorer(name, peers, gossipServer.Ingress(), gossipServer.Outgress(), uiServer.Ingress(), uiServer.Outgress(), debug, antiEntropy)
	}

	// Create the webserver for interacting with the rumorer
	var webServer *WebServer = nil
	if withGUI {
		webServer = NewWebServer(rumorer, uiPort)
	}

	return &Gossiper{
		UIServer:     uiServer,
		GossipServer: gossipServer,
		WebServer:    webServer,
		Rumorer:      rumorer,
		name:         name,
		debug:        debug,
	}
}

func (g *Gossiper) Run() {
	g.UIServer.Run()
	g.GossipServer.Run()
	g.Rumorer.Run()
	if g.WebServer != nil {
		g.WebServer.Run()
	}
}
