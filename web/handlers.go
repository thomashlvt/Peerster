package web

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	"net/http"
)

func (ws *WebServer) handleGetNodeID(w http.ResponseWriter, r *http.Request) {
	// Get the NodeID from the rumorer, encode it, and write it to the GUI
	type respStruct struct {
		Id string `json:"id"`
	}
	err := json.NewEncoder(w).Encode(respStruct{Id: ws.rumorer.Name()})
	if err != nil {
		fmt.Printf("ERROR: could net encode node-id: %v\n", err)
	}
}

func (ws *WebServer) handleGetMessages(w http.ResponseWriter, r *http.Request) {
	// Get all messages from the rumorer, encode them, and write them to the GUI
	type msgStruct struct {
		Origin string `json:"origin"`
		Id     uint32 `json:"id"`
		Text   string `json:"text"`
	}
	type respStruct struct {
		Msgs []msgStruct `json:"msgs"`
	}
	resp := respStruct{Msgs: make([]msgStruct, 0)}
	for _, msg := range ws.rumorer.Messages() {
		resp.Msgs = append(resp.Msgs, msgStruct{msg.Origin, msg.ID, msg.Text})
	}
	err := json.NewEncoder(w).Encode(resp)
	if err != nil {
		fmt.Printf("ERROR: could net encode messages: %v\n", err)
	}
}

func (ws *WebServer) handleGetPeers(w http.ResponseWriter, r *http.Request) {
	// Get all peers from the rumorer, encode them, and return them to the GUI client
	type respStruct struct {
		Peers []string `json:"peers"`
	}
	peers := ws.rumorer.Peers()
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
	// Decode the message and send it to the gossiper over UDP
	decoder := json.NewDecoder(r.Body)
	var data struct {
		Text string `json:"text"`
	}
	err := decoder.Decode(&data)
	if err != nil {
		panic(err)
	}

	// Send message to the Gossiper
	ws.rumorer.UIIn() <- &Message{Text: data.Text}
}

func (ws *WebServer) handlePostPeers(w http.ResponseWriter, r *http.Request) {
	// Decode request, and register peer with the rumorer
	decoder := json.NewDecoder(r.Body)
	var data struct {
		Peer string `json:"peer"`
	}
	err := decoder.Decode(&data)
	if err != nil {
		panic(err)
	}
	ws.rumorer.AddPeer(UDPAddr{data.Peer})
}

func (ws *WebServer) handleGetOrigins(w http.ResponseWriter, r *http.Request) {
	// Get all origins from the private rumorer, encode them, and return them to the GUI client
	type respStruct struct {
		Origins []string `json:"origins"`
	}
	origins := ws.privateRumorer.Origins()
	resp := respStruct{Origins: make([]string, len(origins))}
	for i, origin := range origins {
		resp.Origins[i] = origin
	}
	err := json.NewEncoder(w).Encode(resp)
	if err != nil {
		fmt.Printf("ERROR: could net encode origins: %v\n", err)
	}
}

func (ws *WebServer) handleGetPrivate(w http.ResponseWriter, r *http.Request) {
	// Parse origin from request
	vars := mux.Vars(r)
	origin := vars["origin"]

	// Get all msgs from the private rumorer, encode them, and return them to the GUI client
	type respStruct struct {
		Msgs []string `json:"msgs"`
	}
	msgs := ws.privateRumorer.PrivateMessages(origin)
	err := json.NewEncoder(w).Encode(respStruct{Msgs: msgs})
	if err != nil {
		fmt.Printf("ERROR: could net encode msgs: %v\n", err)
	}
}

func (ws *WebServer) handlePostPrivate(w http.ResponseWriter, r *http.Request) {
	// Parse origin from request
	vars := mux.Vars(r)
	origin := vars["origin"]

	// Decode the message and send it to the gossiper over UDP
	decoder := json.NewDecoder(r.Body)
	var data struct {
		Text string `json:"text"`
	}
	err := decoder.Decode(&data)
	if err != nil {
		panic(err)
	}

	// Send message to the Gossiper
	ws.privateRumorer.UIIn() <- &Message{Text: data.Text, Destination: &origin}
}

func (ws *WebServer) handlePostShareFile(w http.ResponseWriter, r *http.Request) {
	// Decode the message and send it to the gossiper over UDP
	decoder := json.NewDecoder(r.Body)
	var data struct {
		File string `json:"file"`
	}
	err := decoder.Decode(&data)
	if err != nil {
		panic(err)
	}

	// Send message to the Gossiper
	ws.fileHandler.UIIn() <- &Message{File: &data.File}
}


func (ws *WebServer) handleGetFiles(w http.ResponseWriter, r *http.Request) {
	// Get all origins from the private rumorer, encode them, and return them to the GUI client
	type respStruct struct {
		Files []string `json:"files"`
	}
	resp := respStruct{Files: ws.fileHandler.GetFilesFromFilesystem()}

	err := json.NewEncoder(w).Encode(resp)
	if err != nil {
		fmt.Printf("ERROR: could net encode origins: %v\n", err)
	}
}

func (ws *WebServer) handlePostDownloadFile(w http.ResponseWriter, r *http.Request) {
	// Decode the message and send it to the gossiper over UDP
	decoder := json.NewDecoder(r.Body)
	var data struct {
		Hash string `json:"hash"`
		Origin string `json:"origin"`
		FileName string `json:"filename"`
	}
	err := decoder.Decode(&data)
	if err != nil {
		panic(err)
	}
	req, _ := hex.DecodeString(data.Hash)

	// Send message to the Gossiper
	ws.fileHandler.UIIn() <- &Message{File: &data.FileName, Request: &req, Destination: &data.Origin}
}