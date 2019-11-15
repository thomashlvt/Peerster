package main

import (
	"encoding/hex"
	. "github.com/thomashlvt/Peerster/utils"
	"log"
	"os"

	"flag"
	"fmt"
	"github.com/dedis/protobuf"
	"net"
)

var (
	UIPort  string
	msg     string
	dest    string
	file    string
	request string
)

func main() {
	// Load command line arguments
	flag.StringVar(&UIPort, "UIPort", "8080", "port for the UI client (default '8080'")
	flag.StringVar(&msg, "msg", "", "message to be sent; if the -dest flag is present, "+
		"this is a private message, otherwise it’s a rumor message")
	flag.StringVar(&dest, "dest", "", "destination for the private message; can be omitted")
	flag.StringVar(&file, "file", "", "file to be indexed by the gossiper")
	flag.StringVar(&request, "request", "", "request a chunk or metafile of this hash")
	flag.Parse()

	// Types of messages from client:
	// 1. Normal message that will be mongered/broadcast: ONLY msg provided
	// 2. Private message to a private peer: msg AND dest provided
	// 3. File upload: file provided
	if file != "" && msg != "" {
		fmt.Println("Incorrect argument usage")
		os.Exit(1)
	}

	if file != "" && (request != "" && dest == "" || request == "" && dest != "") {
		fmt.Println("Incorrect argument usage")
		os.Exit(1)
	}

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
	if file != "" {
		message.File = &file
	}
	if request != "" {
		req, err := hex.DecodeString(request)
		if err != nil {
			log.Fatal("Could not decode -request string, please make sure it is hexadecimal!")
		}
		message.Request = &req
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
