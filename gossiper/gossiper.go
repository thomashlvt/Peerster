package gossiper

import (
	. "github.com/thomashlvt/Peerster/rumorer"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	. "github.com/thomashlvt/Peerster/web"
)

type Gossiper struct {
	UIServer *Server
	GossipServer *Server
	WebServer *WebServer

	Rumorer GenericRumorer

	name string

	debug bool
}

func NewGossiper(name string, peers *Set, simple bool, uiPort string, gossipAddr string, debug bool) *Gossiper {

	uiServer := NewServer("127.0.0.1:" + uiPort)
	gossipServer := NewServer(gossipAddr)

	var rumorer GenericRumorer

	if simple {
		rumorer = NewSimpleRumorer(gossipAddr, name, peers, gossipServer.Ingress(), gossipServer.Outgress(), uiServer.Ingress(), debug)
	} else {
		rumorer = NewRumorer(name, peers, gossipServer.Ingress(), gossipServer.Outgress(), uiServer.Ingress(), uiServer.Outgress(), debug)
	}

	webServer := NewWebServer(rumorer, uiPort)

	return &Gossiper{
		UIServer: uiServer,
		GossipServer: gossipServer,
		WebServer: webServer,
		Rumorer: rumorer,
		name: name,
		debug: debug,
	}
}

func (g *Gossiper) Run() {
	g.UIServer.Run()
	g.GossipServer.Run()
	g.WebServer.Run()
	g.Rumorer.Run()
}


