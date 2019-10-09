package web

import (
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"


	"encoding/json"
	"fmt"
	"github.com/dedis/protobuf"
	"net"
	"net/http"
)

func (ws *WebServer) handleGetNodeID(w http.ResponseWriter, r *http.Request) {
	type respStruct struct{ Id string `json:"id"` }
	err := json.NewEncoder(w).Encode(respStruct{Id: ws.rumorer.Name()})
	if err != nil {
		fmt.Printf("ERROR: could net encode node-id: %v\n", err)
	}
}

func (ws *WebServer) handleGetMessages(w http.ResponseWriter, r *http.Request) {
	type msgStruct struct{
		Origin string `json:"origin"`
		Id uint32 `json:"id"`
		Text string `json:"text"`

	}
	type respStruct struct{ Msgs []msgStruct `json:"msgs"`}
	resp := respStruct{Msgs: make([]msgStruct, 0)}
	for _, msg := range ws.rumorer.GetMessages() {
		resp.Msgs = append(resp.Msgs, msgStruct{ msg.Origin, msg.ID, msg.Text})
	}
	err := json.NewEncoder(w).Encode(resp)
	if err != nil {
		fmt.Printf("ERROR: could net encode messages: %v\n", err)
	}
}

func (ws *WebServer) handleGetPeers(w http.ResponseWriter, r *http.Request) {
	type respStruct struct{ Peers []string `json:"peers"`}
	peers := ws.rumorer.GetPeers()
	resp := respStruct{Peers: make([]string, len(peers))}
	for i, peer := range peers {
		resp.Peers[i] = peer.String()
	}
	err := json.NewEncoder(w).Encode(resp)
	if err != nil {
		fmt.Printf("ERROR: could net encode peer: %v\n", err)
	}
}

func (ws *WebServer) handlePostMessages(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var data struct {Text string `json:"text"`}
	err := decoder.Decode(&data)
	if err != nil {
		panic(err)
	}

	// Send message to client

	// Set up UDP socket
	addr := "127.0.0.1" + ":" + ws.uiPort
	remoteAddr, err := net.ResolveUDPAddr("udp", addr)
	conn, err := net.DialUDP("udp", nil, remoteAddr)
	if err != nil {
		panic(fmt.Sprintf("ERROR: %v", err))
	}

	// Close connection after message is sent
	defer conn.Close()

	packetBytes, err := protobuf.Encode(&ClientMessage{POST: &data.Text})
	if err != nil {
		fmt.Printf("ERROR: Could not serialize message\n")
		fmt.Println(err)
	}

	_, err = conn.Write(packetBytes)

	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
}

func (ws *WebServer) handlePostPeers(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	var data struct{ Peer string `json:"peer"` }
	err := decoder.Decode(&data)
	if err != nil {
		panic(err)
	}
	ws.rumorer.AddPeer(UDPAddr{data.Peer})
}