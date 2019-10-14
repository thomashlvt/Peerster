package main

import (
	. "github.com/thomashlvt/Peerster/utils"

	"flag"
	"fmt"
	"github.com/dedis/protobuf"
	"net"
)

var (
	UIPort 	string
	msg string
	get bool
)

func main() {
	// Load command line arguments
	flag.StringVar(&UIPort, "UIPort", "8080", "port for the UI client (default '8080'")
	flag.StringVar(&msg, "msg", "", "message to be sent")
	flag.BoolVar(&get, "get", false, "get messages")
	flag.Parse()

	// Send message to Peerster
	if !get {
		addr := "127.0.0.1" + ":" + UIPort
		SendMsg(addr)
	} else {
		GetMsg()
	}
}

func GetMsg() {
	// Set up UDP socket
	addr := "127.0.0.1" + ":" + UIPort
	remoteAddr, err := net.ResolveUDPAddr("udp", addr)
	localAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:9998")
	conn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		panic(fmt.Sprintf("ERROR: %v", err))
	}

	get := "42"
	packetBytes, err := protobuf.Encode(&ClientMessage{GET: &get})
	if err != nil {
		panic(fmt.Sprintf("ERROR: Could not serialize message: %v", err))
	}

	_, err = conn.WriteToUDP(packetBytes, remoteAddr)
	if err != nil {
		panic(fmt.Sprintf("ERROR: %v", err))
	}
	buffer := make([]byte, 1024)
	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		panic(fmt.Sprintf("ERROR: %v", err))
	}
	msgs := Messages{}
	err = protobuf.Decode(buffer[:n], &msgs)
	if err != nil {
		panic(fmt.Sprintf("ERROR: %v", err))
	}
	for _, msg := range msgs.Msgs {
		fmt.Printf("MSG from %v with ID %v: '%v'\n", msg.Origin, msg.ID, msg.Text)
	}
}

// Send message to UDP server on localhost:port
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
	packetBytes, err := protobuf.Encode(&ClientMessage{POST: &msg})
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

