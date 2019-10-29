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
	dest   string
)

func main() {
	// Load command line arguments
	flag.StringVar(&UIPort, "UIPort", "8080", "port for the UI client (default '8080'")
	flag.StringVar(&msg, "msg", "", "message to be sent; if the -dest flag is present, " +
		"this is a private message, otherwise it’s a rumor message")
	flag.StringVar(&dest, "dest", "", "destination for the private message; can be omitted")
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
	message := Message{Text: msg}
	if dest != "" {
		message.Destination = &dest
	}
	packetBytes, err := protobuf.Encode(&message)
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
