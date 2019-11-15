package rumorer

import (
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
)

// Interface that defines all functions the simple/normal rumorer must implement
type GenericRumorer interface {
	Run()

	// Needed for the webserver
	Name() string
	Messages() []*RumorMessage
	Peers() []UDPAddr
	AddPeer(addr UDPAddr)
	UIIn() chan *Message
}
