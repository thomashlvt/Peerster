package gossiper

import (
	. "github.com/thomashlvt/Peerster/rumorer"
	. "github.com/thomashlvt/Peerster/simpleRumorer"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
)

type genericRumorer interface { Run() }

type Gossiper struct {
	UIServer *Server
	GossipServer *Server

	Rumorer genericRumorer

	debug bool
}

func NewGossiper(name string, peers *Set, simple bool, uiPort string, gossipAddr string, debug bool) *Gossiper {

	uiServer := NewServer("127.0.0.1:" + uiPort)
	gossipServer := NewServer(gossipAddr)

	var rumorer genericRumorer

	if simple {
		rumorer = NewSimpleRumorer(gossipAddr, name, peers, gossipServer.Ingress(), gossipServer.Outgress(), uiServer.Ingress(), debug)
	} else {
		rumorer = NewRumorer(name, peers, gossipServer.Ingress(), gossipServer.Outgress(), uiServer.Ingress(), uiServer.Outgress(), debug)
	}

	return &Gossiper{
		UIServer: uiServer,
		GossipServer: gossipServer,
		Rumorer: rumorer,
		debug: debug,
	}
}

func (g *Gossiper) Run() {
	g.UIServer.Run()
	g.GossipServer.Run()
	g.Rumorer.Run()
}


