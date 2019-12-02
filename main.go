package main

import (
	. "github.com/thomashlvt/Peerster/constants"
	. "github.com/thomashlvt/Peerster/gossiper"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	"math/rand"
	"time"

	"flag"
	"fmt"
	"strings"
)

var (
	// The variables below will be filled in by the CLI arguments
	uiPort        string
	gossipAddr    string
	name          string
	peers         string
	simple        bool
	antiEntropy   int
	routeRumoring int

	debug         bool
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
	flag.IntVar(&antiEntropy, "antiEntropy", 10, "Timeout for running anti entropy")
	flag.IntVar(&routeRumoring, "rtimer", 0, "Timeout in seconds to send route rumors. 0 (default) "+
		"means disable sending route rumors.")
	flag.Parse()

	// Seed random generator
	rand.Seed(time.Now().UTC().UnixNano())

	// Parse the arguments
	if name == "" {
		panic(fmt.Sprintln("Please provide your name with the '-name' flag"))
	}
	peersSet := NewSet()
	for _, peer := range strings.Split(peers, ",") {
		if peer != "" {
			peersSet.Add(UDPAddr{Addr: peer})
		}
	}

	Debug = debug
	HW1 = false
	HW2 = true

	// Initialize and run gossiper
	goss := NewGossiper(name, peersSet, simple, uiPort, gossipAddr, antiEntropy, routeRumoring)
	goss.Run()

	// Wait forever
	select {}
}

