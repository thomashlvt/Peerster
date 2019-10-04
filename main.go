package main

import (
	. "github.com/thomashlvt/Peerster/gossiper"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"

	"flag"
	"fmt"
	"strings"
)

var (
	uiPort 	string
	gossipAddr string
	name string
	peers string
	simple bool
	debug bool
)

func main() {
	// Load command line arguments
	flag.StringVar(&uiPort, "UIPort", "8080", "port for the UI client (default '8080'")
	flag.StringVar(&gossipAddr, "gossipAddr", "127.0.0.1:5000",
			"ip:port for the gossiper (default '127.0.0.1:5000")
	flag.StringVar(&name, "name", "", "name of the gossiper")
	flag.StringVar(&peers, "peers", "", "comma seperated list of peers in the from ip:port")
	flag.BoolVar(&simple, "simple", false, "run gossiper in simple broadcast mode")
	flag.BoolVar(&debug, "debug", false, "print debug information")
	flag.Parse()

	if name == "" {
		fmt.Println("Please provide your name with the '-name' flag")
	}

	peersSet := NewSet()
	for _, peer := range strings.Split(peers, ",") {
		if peer != "" {
			peersSet.Add(UDPAddr{Addr: peer})
		}
	}

	// Add peers to known peers list
	goss := NewGossiper(name, peersSet, simple, uiPort, gossipAddr, debug)

	goss.Run()

	// wait forever by blocking on empty channel
	wait := make(chan struct{})
	<- wait
}

// TODO:
//   * Timeout: exclude randPeer
//   * Webapp