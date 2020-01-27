package main

import (
	. "github.com/thomashlvt/Peerster/constants"
	. "github.com/thomashlvt/Peerster/gossiper"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	"log"
	"math/rand"
	"time"

	"flag"
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
	hw3ex2		  bool
	hw3ex3        bool
	N             int
	stubbornTimeout int
	hopLimit	 int
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
	flag.BoolVar(&hw3ex2, "hw3ex2", false, "Run in HW3 EX2 mode")
	flag.IntVar(&N, "N", -1, "Total number of peers in the network")
	flag.IntVar(&stubbornTimeout, "stubbornTimeout", 5, "Timeout for resending txn BlockPublish")
	flag.IntVar(&hopLimit, "hopLimit", 10, "HopLimit for point to point messages")
	flag.BoolVar(&hw3ex3, "hw3ex3", false, "Run peerster in HW3 - EX3 mode")
	flag.Parse()

	// Seed random generator
	rand.Seed(time.Now().UTC().UnixNano())

	// Parse the arguments
	if name == "" {
		log.Fatal("Please provide your name with the '-name' flag")
	}
	peersSet := NewSet()
	for _, peer := range strings.Split(peers, ",") {
		if peer != "" {
			peersSet.Add(UDPAddr{Addr: peer})
		}
	}
	if N == -1 && hw3ex2 {
		log.Fatal("Please provide the total number of peers in the network")
	}

	// Set constants
	Debug = debug
	HW1 = true
	HW2 = true
	HW3EX2 = hw3ex2
	HW3EX3 = hw3ex3

	// Initialize and run gossiper
	goss := NewGossiper(name, peersSet, simple, uiPort, gossipAddr, antiEntropy, routeRumoring, N, stubbornTimeout, hopLimit)
	goss.Run()

	// Wait forever
	select {}
}

