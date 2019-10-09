package udp

import (
	"fmt"
	"net"
)

type UDPAddr struct {
	Addr string
}

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

type Packet struct {
	Addr UDPAddr
	Data []byte
}

type Server struct {
	ingress chan *Packet
	outgress chan *Packet

	address *net.UDPAddr
	conn *net.UDPConn
}

func (s *Server) Ingress() chan *Packet {
	return s.ingress
}

func (s *Server) Outgress() chan *Packet {
	return s.outgress
}

func NewServer(addr string) *Server {
	addrRes := UDPAddr{addr}.Resolve()
	ln, err := net.ListenUDP("udp", addrRes)
	if err != nil {
		panic(fmt.Sprintf("ERROR when setting up UDP socket: %v", err))
	}

	in := make(chan *Packet)
	out := make(chan *Packet)

	return &Server{
		address: addrRes,
		conn: ln,
		ingress: in,
		outgress: out,
	}
}

func (s *Server) Listen() {
	for {
		buffer := make([]byte, 1024)
		n, addr, err := s.conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Printf("ERROR when reading from connection: %v", err)
		}
		fmt.Printf("[DEBUG] Packet received from %v\n", addr)
		s.ingress <- &Packet{UDPAddr{addr.String()},buffer[:n]}
	}
}

func (s *Server) Talk() {
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
