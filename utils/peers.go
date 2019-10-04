package utils

import (
	. "github.com/thomashlvt/Peerster/udp"

	"fmt"
	"math/rand"
	"sync"
)

type DebugMutex struct {
	m *sync.RWMutex
}

func (m *DebugMutex) Lock() {
	fmt.Println("Locking...")
	//m.m.Lock()
}

func (m *DebugMutex) Unlock() {
	fmt.Println("Unlocking...")
	//m.m.Unlock()
}

func (m *DebugMutex) RLock() {
	fmt.Println("RLocking...")
	//m.m.RLock()
}

func (m *DebugMutex) RUnlock() {
	fmt.Println("RUnlocking...")
	//m.m.RUnlock()
}

type Set struct {
	data map[UDPAddr]bool
	dataMutex *sync.RWMutex
}

func NewSet() *Set {
	return &Set{
		data: make(map[UDPAddr]bool),
		dataMutex: &sync.RWMutex{},
	}
}

func (s *Set) Add(el UDPAddr) {
	s.dataMutex.Lock()
	defer s.dataMutex.Unlock()

	s.data[el] = true
}

func (s *Set) Delete(el UDPAddr) {
	s.dataMutex.Lock()
	defer s.dataMutex.Unlock()

	if s.data[el] {
		delete(s.data, el)
	}
}

func (s *Set) Contains(el UDPAddr) bool {
	s.dataMutex.RLock()
	defer s.dataMutex.RUnlock()

	return s.data[el]
}

func (s *Set) Data() []UDPAddr {
	s.dataMutex.RLock()
	defer s.dataMutex.RUnlock()

	all := make([]UDPAddr, len(s.data))
	i:= 0
	for peer, _ := range s.data {
		all[i] = peer
		i += 1
	}
	return all
}

func (s *Set) String() string {
	s.dataMutex.RLock()
	defer s.dataMutex.RUnlock()

	res := ""
	for k, v := range s.data {
		if v {
			res += fmt.Sprintf("%v,", k)
		}
	}
	return res[:len(res)-1]
}

func (s *Set) Rand() (UDPAddr, bool) {
	s.dataMutex.RLock()
	defer s.dataMutex.RUnlock()
	if len(s.data) == 0 {
		return UDPAddr{}, false
	}

	i := rand.Intn(len(s.data))
	for k := range s.data {
		if i == 0 {
			return k, true
		}
		i--
	}
	panic("Unreachable")
}

func (s *Set) Len() int {
	return len(s.data)
}

func (s *Set) RandExcept(except UDPAddr) (UDPAddr, bool) {
	if (s.Len() == 1 && s.Contains(except)) || s.Len() == 0 {
		return UDPAddr{}, false
	}

	for {
		randPeer, ok := s.Rand()
		if !ok {
			return UDPAddr{}, false
		}
		if randPeer != except {
			return randPeer, true
		}
	}
}