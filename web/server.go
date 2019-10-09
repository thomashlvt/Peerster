package web

import (
	. "github.com/thomashlvt/Peerster/rumorer"

	"github.com/gorilla/mux"
	"log"
	"net/http"
	"time"
)

type WebServer struct {
	rumorer GenericRumorer
	router *mux.Router
	server *http.Server

	uiPort string
}

func NewWebServer(rumorer GenericRumorer, uiPort string) (ws *WebServer) {
	ws = &WebServer{}
	ws.uiPort = uiPort
	ws.rumorer = rumorer
	ws.router = mux.NewRouter()

	// Serve api calls
	ws.router.HandleFunc("/node-id", ws.handleGetNodeID).Methods("GET")
	ws.router.HandleFunc("/messages", ws.handleGetMessages).Methods("GET")
	ws.router.HandleFunc("/peers", ws.handleGetPeers).Methods("GET")
	ws.router.HandleFunc("/messages", ws.handlePostMessages).Methods("POST")
	ws.router.HandleFunc("/peers", ws.handlePostPeers).Methods("POST")

	// Serve static files (Note: relative path from Peerste root)
	ws.router.PathPrefix("/").Handler(http.StripPrefix("/", http.FileServer(http.Dir("web/assets"))))

	ws.server = &http.Server{
		Handler: ws.router,
		Addr:    "127.0.0.1:8080",
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	return ws
}

func (ws *WebServer) Run() {
	go func() {
		log.Fatal(ws.server.ListenAndServe())
	}()
}
