package gossiper

import (
	. "github.com/thomashlvt/Peerster/files"
	. "github.com/thomashlvt/Peerster/privateRumorer"
	. "github.com/thomashlvt/Peerster/rumorer"
	. "github.com/thomashlvt/Peerster/search"
	. "github.com/thomashlvt/Peerster/utils"
	. "github.com/thomashlvt/Peerster/web"
)

type Gossiper struct {
	Dispatcher *Dispatcher

	WebServer *WebServer

	Rumorer GenericRumorer

	PrivateRumorer *PrivateRumorer

	FileHandler *FileHandler

	Searcher *Searcher

	name string
}

func NewGossiper(name string, peers *Set, simple bool, uiPort string, gossipAddr string,
	antiEntropy int, routeRumoringTimeout int) *Gossiper {
	// Create the dispatcher
	disp := NewDispatcher(uiPort, gossipAddr)

	// Create the simple/normal rumorer
	var rumorer GenericRumorer
	if simple {
		rumorer = NewSimpleRumorer(gossipAddr, name, peers, disp.RumorerGossipIn, disp.RumorerOut, disp.RumorerUIIn)
	} else {
		rumorer = NewRumorer(name, peers, disp.RumorerGossipIn, disp.RumorerOut, disp.RumorerUIIn, antiEntropy)
	}

	// Create the rumorer for private messages
	privateRumorer := NewPrivateRumorer(name, disp.PrivateRumorerGossipIn, disp.PrivateRumorerUIIn,
		disp.PrivateRumorerGossipOut, disp.RumorerUIIn, disp.PrivateRumorerLocalOut, routeRumoringTimeout, gossipAddr)

	fileHandler := NewFileHandler(name, disp.FileHandlerIn, disp.FileHandlerUIIn, disp.PrivateRumorerGossipIn)
	files, filesMutex := fileHandler.Files()

	searcher := NewSearcher(name, peers, disp.SearchHandlerIn, disp.SearchHandlerOut, disp.SearchHandlerUIIn,
		disp.PrivateRumorerGossipIn, fileHandler.SearchDownloadIn(), files, filesMutex)

	// Create the webserver for interacting with the rumorer
	webServer := NewWebServer(rumorer, privateRumorer, fileHandler, uiPort)

	return &Gossiper{
		Dispatcher:     disp,
		WebServer:      webServer,
		Rumorer:        rumorer,
		PrivateRumorer: privateRumorer,
		FileHandler:    fileHandler,
		Searcher:       searcher,
		name:           name,
	}
}

func (g *Gossiper) Run() {
	g.Dispatcher.Run()
	g.Rumorer.Run()
	g.PrivateRumorer.Run()
	g.FileHandler.Run()
	g.Searcher.Run()
	if g.WebServer != nil {
		g.WebServer.Run()
	}
}
