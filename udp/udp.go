package udp

import (
	"fmt"
	"net"
)

// Wrapper type that represents a UDP address
type UDPAddr struct {
	Addr string
}

// Resolve this wrapper type to a *net.UDPAddr
func (a UDPAddr) Resolve() *net.UDPAddr {
	addr, err := net.ResolveUDPAddr("udp4", a.Addr)
	if err != nil {
		panic(fmt.Sprintf("ERROR when resolving UDP address: %v", err))
	}
	return addr
}

func (a UDPAddr) String() string {
	return a.Addr
}

// These packets are used to send Data bytes to Addr over the in/out communication channels of the server
type RawPacket struct {
	Addr UDPAddr
	Data []byte
}

// Server that listens on a UDP socket and sends the Packets it receives to the ingress channel, and
// listens on the outgress channel and sends this data outward through the same UDP socket
type Server struct {
	ingress chan *RawPacket
	outgress chan *RawPacket

	address *net.UDPAddr
	conn *net.UDPConn
}

func (s *Server) Ingress() chan *RawPacket {
	return s.ingress
}

func (s *Server) Outgress() chan *RawPacket {
	return s.outgress
}

func NewServer(addr string) *Server {
	addrRes := UDPAddr{addr}.Resolve()
	ln, err := net.ListenUDP("udp", addrRes)
	if err != nil {
		panic(fmt.Sprintf("ERROR when setting up UDP socket: %v", err))
	}

	in := make(chan *RawPacket)
	out := make(chan *RawPacket)

	return &Server{
		address: addrRes,
		conn: ln,
		ingress: in,
		outgress: out,
	}
}

func (s *Server) Listen() {
	// Put incoming messages in the ingress channel
	for {
		buffer := make([]byte, 4096)
		n, addr, err := s.conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Printf("ERROR when reading from connection: %v", err)
		}
		s.ingress <- &RawPacket{UDPAddr{addr.String()},buffer[:n]}
	}
}

func (s *Server) Talk() {
	// Send outgoing messages through the UDP socket
	for {
		data := <- s.outgress
		_, err := s.conn.WriteToUDP(data.Data, data.Addr.Resolve())
		if err != nil {
			panic(fmt.Sprintf("ERROR could not send bytes over UDP: %v", err))
		}
	}
}

func (s *Server) Run() {
	go s.Listen()
	go s.Talk()
}
