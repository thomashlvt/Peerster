package main

import (
	"flag"
	"fmt"
	"github.com/dedis/protobuf"
	"net"
)

var (
	UIPort 	string
	msg string
)

func main() {
	// Load command line arguments
	flag.StringVar(&UIPort, "UIPort", "8080", "port for the UI client (default '8080'")
	flag.StringVar(&msg, "msg", "", "message to be sent")

	flag.Parse()

	// Send message to Peerster
	sendMsg(msg, UIPort)
}

// Message format that will be marshalled
type Message struct {
	Text string
}

// Send message to UDP server on localhost:port
func sendMsg(msg string, port string) {
	// Set up UDP socket
	addr := "127.0.0.1" + ":" + port
	remoteAddr, err := net.ResolveUDPAddr("udp", addr)
	conn, err := net.DialUDP("udp", nil, remoteAddr)

	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}

	// Close connection after message is sent
	defer conn.Close()

	packetBytes, err := protobuf.Encode(&Message{msg})
	if err != nil {
		fmt.Printf("ERROR: Could not serialize message\n")
		fmt.Println(err)
	}

	_, err = conn.Write(packetBytes)

	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
}

