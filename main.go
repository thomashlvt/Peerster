package main

import (
	"flag"
	"fmt"
	"github.com/dedis/protobuf"
	"net"
	"strings"
)

var (
	// Flags from command line
	UIPort 	string
	gossipPort string
	gossipAddr string
	name string
	peers string
	simple bool

	KnownPeers map[string]bool

)

// Message format from the client
type Message struct {
	Text string
}

// Message format for Peersters talking to each other
type SimpleMessage struct {
	OriginalName string
	RelayPeerAddr string
	Contents string
}

// Messages are wrapped in a packet before being sent
type GossipPacket struct {
	Simple *SimpleMessage
}

func main() {
	// Load command line arguments
	flag.StringVar(&UIPort, "UIPort", "8080", "port for the UI client (default '8080'")
	flag.StringVar(&gossipAddr, "gossipAddr", "127.0.0.1:5000",
			"ip:port for the gossiper (default '127.0.0.1:5000")
	flag.StringVar(&name, "name", "", "name of the gossiper")
	flag.StringVar(&peers, "peers", "", "comma seperated list of peers in the from ip:port")
	flag.BoolVar(&simple, "simple", false, "run gossiper in simple broadcast mode")

	flag.Parse()

	// Get gossip port from cli arguments
	gossipPort = strings.Split(gossipAddr, ":")[1]

	if name == "" {
		fmt.Println("Please provide your name with the '-name' flag")
	}

	// Add peers to known peers list
	KnownPeers = make(map[string]bool)
	for _, peer := range strings.Split(peers, ",") {
		if peer != "" && !KnownPeers[peer] {
			KnownPeers[peer] = true
		}
	}

	go startUDPServer(UIPort, handleUIConn)
	go startUDPServer(gossipPort, handlePeerConn)

	// Wait forever by blocking on empty channel
	wait := make(chan struct{})
	<- wait
}

// Fn that will be used to process incoming UDP packets
type callback func(*net.UDPConn)

func startUDPServer(port string, handleConn callback) {
	// Set updAddr
	service := "127.0.0.1" + ":" + port
	udpAddr, err := net.ResolveUDPAddr("udp4", service)
	if err != nil {
		fmt.Printf("ERROR when resolving UDP address: '%v'\n", err)
		return
	}

	// setup listener for incoming UDP connection
	ln, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		fmt.Printf("ERROR when setting up UDP socket: '%v'\n", err)
		return
	}

	fmt.Printf("UDP server up and listening on port '%v'\n", port)

	defer ln.Close()

	for {
		// wait for UDP client to connect
		handleConn(ln)
	}
}

func setToString(set map[string]bool) string {
	res := ""
	for k, v := range set {
		if v {
			res += k + ","
		}
	}
	return res[:len(res)-1]
}

// Handles requests for the UI server
func handleUIConn(conn *net.UDPConn) {
	buffer := make([]byte, 1024)

	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		fmt.Printf("ERROR when reading from connection: '%v'\n", err)
	}

	msg := Message{}
	err = protobuf.Decode(buffer[:n], &msg)
	if err != nil {
		fmt.Printf("ERROR when decoding packet: '%v'\n", err)
		return
	}

	fmt.Printf("CLIENT MESSAGE %v\n", msg.Text)
	fmt.Printf("PEERS %v\n", setToString(KnownPeers))

	simpleMsg := SimpleMessage{
		name,
		gossipAddr,
		msg.Text,
	}

	packet := GossipPacket{&simpleMsg}
	// gossip message to all known peers
	for peer, val := range KnownPeers {
		if val {
			sendPacket(&packet, peer)
		}
	}
}

// Handles requests for the Peerster server
func handlePeerConn(conn *net.UDPConn) {
	buffer := make([]byte, 1024)

	n, _, err := conn.ReadFromUDP(buffer)
	if err != nil {
		fmt.Printf("ERROR when reading from connection: '%v'\n", err)
	}

	packet := GossipPacket{}
	err = protobuf.Decode(buffer[:n], &packet)
	if err != nil {
		fmt.Printf("ERROR when decoding packet: '%v'\n", err)
		return
	}

	sender := packet.Simple.RelayPeerAddr

	// Store relay in set of known peers
	if !KnownPeers[sender] {
		KnownPeers[sender] = true
	}

	fmt.Printf("SIMPLE MESSAGE origin %v from %v contents %v\n",
		packet.Simple.OriginalName, packet.Simple.RelayPeerAddr, packet.Simple.Contents)
	fmt.Printf("PEERS %v\n", setToString(KnownPeers))

	// Change relay address to own address
	packet.Simple.RelayPeerAddr = gossipAddr

	// gossip message to all known peers
	for peer, val := range KnownPeers {
		if val && peer != sender {
			sendPacket(&packet, peer)
		}
	}
}

// send a GossipPacket to the peer at addr
func sendPacket(packet *GossipPacket, addr string) {
	// Set up UDP socket
	remoteAddr, err := net.ResolveUDPAddr("udp", addr)
	conn, err := net.DialUDP("udp", nil, remoteAddr)

	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return
	}

	// Close connection after message is sent
	defer conn.Close()

	packetBytes, err := protobuf.Encode(packet)
	if err != nil {
		fmt.Printf("ERROR: Could not serialize GossipPacket\n")
		fmt.Println(err)
	}

	_, err = conn.Write(packetBytes)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
}
