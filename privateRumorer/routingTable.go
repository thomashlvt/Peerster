package privateRumorer

import (
	"fmt"
	. "github.com/thomashlvt/Peerster/udp"
	"sync"
)

type RoutingTable struct {
	lock      *sync.RWMutex
	table     map[string]UDPAddr
	recentIDs map[string]uint32
}

func NewRoutingTable() *RoutingTable {
	return &RoutingTable{
		lock:      &sync.RWMutex{},
		table:     make(map[string]UDPAddr),
		recentIDs: make(map[string]uint32),
	}
}

func (rt *RoutingTable) Origins() []string {
	// Thread safe access to the routing table
	rt.lock.RLock()
	defer rt.lock.RUnlock()

	res := make([]string, len(rt.table))
	i := 0
	for key, _ := range rt.table {
		res[i] = key
		i += 1
	}
	return res
}

func (rt *RoutingTable) Add(origin string, addr UDPAddr, id uint32, printDSDV bool) {
	// Thread safe access to the routing table
	rt.lock.Lock()
	defer rt.lock.Unlock()

	// Check previous entry
	prevId, exists := rt.recentIDs[origin]

	// Update entry
	if !exists || id > prevId {

		rt.recentIDs[origin] = id
		if printDSDV {
			fmt.Printf("DSDV %s %s\n", origin, addr)
		}
		rt.table[origin] = addr
	}
}

func (rt *RoutingTable) Get(origin string) (UDPAddr, bool) {
	// Thread safe access to the routing table
	rt.lock.RLock()
	defer rt.lock.RUnlock()

	if res, ok := rt.table[origin]; ok {
		return res, true
	} else {
		return UDPAddr{}, false
	}
}
