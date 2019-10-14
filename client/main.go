package main

import (
	. "github.com/thomashlvt/Peerster/utils"

	"flag"
	"fmt"
	"github.com/dedis/protobuf"
	"net"
)

var (
	UIPort string
	msg    string
)

func main() {
	// Load command line arguments
	flag.StringVar(&UIPort, "UIPort", "8080", "port for the UI client (default '8080'")
	flag.StringVar(&msg, "msg", "", "message to be sent")
	flag.Parse()

	// Send message to the Gossiper
	addr := "127.0.0.1" + ":" + UIPort
	SendMsg(addr)
}

func SendMsg(addr string) {
	// Set up UDP socket
	remoteAddr, err := net.ResolveUDPAddr("udp", addr)
	conn, err := net.DialUDP("udp", nil, remoteAddr)
	if err != nil {
		panic(fmt.Sprintf("ERROR: %v", err))
	}
	// Close connection after message is sent
	defer conn.Close()

	// Encode the message
	packetBytes, err := protobuf.Encode(&Message{msg})
	if err != nil {
		fmt.Printf("ERROR: Could not serialize message\n")
		fmt.Println(err)
	}

	// Write the bytes to the UDP socket
	_, err = conn.Write(packetBytes)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
}
