package rumorer

import (
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
)

type GenericRumorer interface {
	Name() string
	Run()
	GetMessages() []*RumorMessage
	GetPeers() []UDPAddr
	AddPeer(addr UDPAddr)
}
