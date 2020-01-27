package confirmationRumorer

import (
	"encoding/hex"
	"fmt"
	. "github.com/thomashlvt/Peerster/constants"
	. "github.com/thomashlvt/Peerster/files"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	"sync"
	"time"
)

type TLCMessageWithReplyChan struct {
	Msg *TLCMessage
	ReplyChan chan uint32
}


type ConfirmedGossipInfo struct {
	Origin string
	ID int
	Size int64
	Filename string
	Meta []byte
}


type RoundInfo struct {
	RoundNum int
	BasedOn string
}


type ConfirmationRumorer struct {
	fileIn chan *FileWithReplyChan
	tlcIn chan *TLCMessage
	ackIn chan *AddrGossipPacket

	p2pOut chan *AddrGossipPacket
	tlcMsgOut chan *TLCMessageWithReplyChan

	collectedAcks map[uint32] chan *TLCAck
	collectedAcksMutex *sync.RWMutex

	name string
	N int

	hopLimit uint32
	stubbornTimeout int

	my_time int

	clientQueue chan *FileWithReplyChan
	nextRound chan bool
	gotMajority chan bool

	peerRounds map[string]map[uint32]bool
	peerRoundsMutex *sync.RWMutex

	confirmedPeers map[string]uint32
	confirmedPeersMutex *sync.RWMutex

	claimedFileNames map[string]bool
	claimedFileNamesMutex *sync.RWMutex

	confirmedGossips []ConfirmedGossipInfo
	confirmedGossipsMutex *sync.RWMutex

	rounds []RoundInfo
	roundsMutex *sync.RWMutex
}


func NewConfirmationRumorer(name string, fileIn chan *FileWithReplyChan, tlcIn chan *TLCMessage, ackIn chan *AddrGossipPacket,
	p2pOut chan *AddrGossipPacket, tlcMsgOut chan *TLCMessageWithReplyChan, N int, hopLimit int, stubbornTimeout int) *ConfirmationRumorer {
	return &ConfirmationRumorer{
		name:     name,

		fileIn:   fileIn,
		tlcIn:    tlcIn,
		ackIn:    ackIn,

		p2pOut:   p2pOut,
		tlcMsgOut: tlcMsgOut,

		collectedAcks: make(map[uint32] chan *TLCAck),
		collectedAcksMutex: &sync.RWMutex{},

		N:        N,

		hopLimit: uint32(hopLimit),
		stubbornTimeout: stubbornTimeout,

		my_time: 0,

		clientQueue: make(chan *FileWithReplyChan, 50),
		gotMajority: make(chan bool),

		peerRounds: make(map[string]map[uint32]bool),
		peerRoundsMutex: &sync.RWMutex{},

		confirmedPeers: make(map[string]uint32),
		confirmedPeersMutex: &sync.RWMutex{},

		claimedFileNames: make(map[string]bool),
		claimedFileNamesMutex: &sync.RWMutex{},

		confirmedGossips: make([]ConfirmedGossipInfo, 0),
		confirmedGossipsMutex: &sync.RWMutex{},

		rounds: make([]RoundInfo, 0),
		roundsMutex: &sync.RWMutex{},
	}
}


func (cr *ConfirmationRumorer) ConfirmedRumors() []ConfirmedGossipInfo {
	cr.confirmedGossipsMutex.RLock()
	defer cr.confirmedGossipsMutex.RUnlock()

	return cr.confirmedGossips
}


func (cr *ConfirmationRumorer) Rounds() []RoundInfo {
	cr.roundsMutex.RLock()
	defer cr.roundsMutex.RUnlock()

	return cr.rounds
}

func (cr *ConfirmationRumorer) Run() {
	if HW3EX3 {
		cr.RunProcessClientQueue()
	}

	go func() {
		for {
			select {
			case file := <-cr.fileIn:
				if HW3EX2 {
					go cr.handleNewFile(file, nil, nil)
				} else if HW3EX3 {
					// enqueue client request
					if Debug {
						fmt.Printf("[DEBUG] Added to queue: %v\n", file.File.Name)
					}
					cr.clientQueue <- file

				}
			case tlc := <- cr.tlcIn:
				if tlc.Confirmed == -1 {
					go cr.handleUnconfirmedTLCMessage(tlc)
				} else {
					go cr.handleConfirmedTLCMessage(tlc)
				}
			case packet := <- cr.ackIn:
				go cr.handleAck(packet.Address, packet.Gossip.Ack)
			}
		}
	}()
}


func (cr *ConfirmationRumorer) RunProcessClientQueue() {
	go func() {
		var nextFile *FileWithReplyChan = nil
		for {
			var file *FileWithReplyChan
			if nextFile == nil {
				file = <- cr.clientQueue
			} else {
				// We already took the next file out of the queue
				file = nextFile
			}

			if Debug {
				fmt.Printf("[DEBUG] ClientQueue: Processing new file: %v\n", file.File.Name)
			}

			doneC := make(chan bool)
			interruptC := make(chan bool)
			go cr.handleNewFile(file, doneC, interruptC)

			select {
			case <- doneC:
				if Debug {
					fmt.Printf("[DEBUG] Done confirming file\n")
				}
				// File has been confirmed, now wait for majority to move onto the next round
				<- cr.gotMajority
				if Debug {
					fmt.Printf("[DEBUG] Got majority\n")
				}
			case <- cr.gotMajority:
				if Debug {
					fmt.Printf("[DEBUG] Got majority\n")
				}
				// We have a majority, now wait for the file to be confirmed, or for the next
				// file from the client to move onto the next round
				select {
				case nextFile = <- cr.clientQueue:
					if Debug {
						fmt.Printf("[DEBUG] Got Next file\n")
					}
					// The next file came before the current file was done processing, file confirmation
					// failed: move onto next round, and let file handler now that file name is considered invalid
					interruptC <- true
					file.ReplyChan <- false

				case <- doneC:
					if Debug {
						fmt.Printf("[DEBUG] Done confirming file\n")
					}
					// File was confirmed, and we confirmed a majority of files: move on to the next round
				}
			}

			// End of the round
			cr.finishRound()
		}
	}()
}


func (cr *ConfirmationRumorer) printConfirmedPeers() {
	cr.confirmedPeersMutex.Lock()
	defer cr.confirmedPeersMutex.Unlock()

	toPrint := fmt.Sprintf("ADVANCING TO round %v BASED ON CONFIRMED MESSAGES ", cr.my_time)

	confirmedMsgsStr := ""
	i := 0
	for origin, id := range cr.confirmedPeers {
		confirmedMsgsStr += fmt.Sprintf("origin%v %v ID%v %v, ", i, origin, i, id)
		i += 1
	}
	if i > 0 {
		confirmedMsgsStr = confirmedMsgsStr[:len(confirmedMsgsStr)-2]
	}

	toPrint += confirmedMsgsStr
	fmt.Println(toPrint)

	cr.roundsMutex.Lock()
	cr.rounds = append(cr.rounds, RoundInfo{
		RoundNum: cr.my_time,
		BasedOn:  confirmedMsgsStr,
	})
	cr.roundsMutex.Unlock()
}

func (cr *ConfirmationRumorer) finishRound() {
	// Increase the round number
	cr.my_time += 1
	// Print the advancing to round msg
	cr.printConfirmedPeers()
	// Reset the peers that have sent a confirmation for the next round
	cr.confirmedPeers = make(map[string]uint32)
}


func (cr *ConfirmationRumorer) handleNewFile(file *FileWithReplyChan, doneC chan bool, interruptC chan bool) {
	// First check locally that we have not used the filename yet
	if !cr.checkFileName(file.File.Name) {
		file.ReplyChan <- false
	}

	cr.collectedAcksMutex.Lock()

	// Channel to receive acks on
	ackChan := make(chan *TLCAck)

	// Broadcast transaction to all peers
	replyChan := make(chan uint32)
	cr.tlcMsgOut <- cr.createTLCMessageWithReply(file.File, 0, -1, replyChan)

	// Get the ID
	id := <-replyChan

	// Register channel to receive acks on
	cr.collectedAcks[id] = ackChan

	cr.collectedAcksMutex.Unlock()

	fmt.Printf("UNCONFIRMED GOSSIP origin %v ID %v file name %v size %v metahash %v\n",
		cr.name, id, file.File.Name, file.File.Size, hex.EncodeToString(file.File.Hash[:]))

	// Keep a set of received acks in order to avoid duplicates
	acks := make(map[string] bool)
	acks[cr.name] = true  // we automatically acknowledge our own message
	numAcks := 1

	// Listen for acks
	ticker := time.NewTicker(time.Second * time.Duration(cr.stubbornTimeout))
	done := false
	for !done {
		select {
		case <- ticker.C:
			// Resend the TLCMessage
			cr.tlcMsgOut <- cr.createTLCMessageWithReply(file.File, id, -1, nil)
			ticker = time.NewTicker(time.Second * time.Duration(cr.stubbornTimeout))

		case ack := <- ackChan:
			// Register the Ack
			if _, exists := acks[ack.Origin]; !exists {
				acks[ack.Origin] = true
				numAcks += 1
			}

			// Check if we reached a majority
			if numAcks > cr.N / 2 + 1 {
				done = true
			}

		case <- interruptC:
			// Indicate that file has not been confirmed
			file.ReplyChan <- false

			close(ackChan)
			cr.collectedAcksMutex.Lock()
			delete(cr.collectedAcks, id)
			cr.collectedAcksMutex.Unlock()
			return
		}
	}

	close(ackChan)
	cr.collectedAcksMutex.Lock()
	delete(cr.collectedAcks, id)
	cr.collectedAcksMutex.Unlock()

	// Confirm file
	cr.confirmFile(file.File.Name, file.ReplyChan)

	fmt.Printf("CONFIRMED GOSSIP origin %v ID %v file name %v size %v metahash %v\n",
		cr.name, id, file.File.Name, file.File.Size, hex.EncodeToString(file.File.Hash[:]))

	cr.confirmedGossipsMutex.Lock()
	cr.confirmedGossips = append(cr.confirmedGossips, ConfirmedGossipInfo{
		Origin:   cr.name,
		ID:       int(id),
		Size:     file.File.Size,
		Filename: file.File.Name,
		Meta:      file.File.Hash[:],
	})
	cr.confirmedGossipsMutex.Unlock()

	// Broadcast TLCMessage confirmation
	cr.printRebroadcast(id, acks)

	replyChanConf := make(chan uint32)
	cr.tlcMsgOut <- cr.createTLCMessageWithReply(file.File, 0, int(id), replyChanConf)
	idConf := <- replyChanConf

	// Mark ourselves as confirmed
	cr.markConfirmedPeer(cr.name, idConf)

	if doneC != nil {
		doneC <- true
	}
}


func (cr *ConfirmationRumorer) printRebroadcast(id uint32, acks map[string]bool) {
	toPrint := fmt.Sprintf("RE-BROADCAST ID %v WITNESSES ", id)
	for peer, _ := range acks {
		toPrint += peer + ","
	}
	toPrint = toPrint[:len(toPrint)-1] // remove last comma
	fmt.Println(toPrint)
}


func (cr *ConfirmationRumorer) confirmFile(name string, replyChan chan bool) {
	// Store the new claimed file name
	cr.claimedFileNamesMutex.Lock()
	cr.claimedFileNames[name] = true
	cr.claimedFileNamesMutex.Unlock()

	if Debug {
		cr.printClaimedFiles()
	}

	// Let the file handler know that the file name is valid
	if replyChan != nil {
		replyChan <- true
	}
}


func (cr *ConfirmationRumorer) printClaimedFiles() {
	// Helper function to print the files that are taken
	cr.claimedFileNamesMutex.RLock()
	toPrint := "[DEBUG] FILES: "
	for name := range cr.claimedFileNames {
		toPrint += name + ","
	}
	fmt.Println(toPrint)
	cr.claimedFileNamesMutex.RUnlock()
}


func (cr *ConfirmationRumorer) checkFileName(name string) bool {
	// Returns true if valid filename
	cr.claimedFileNamesMutex.RLock()
	defer cr.claimedFileNamesMutex.RUnlock()

	_, exists := cr.claimedFileNames[name]
	if Debug && exists {
		fmt.Printf("[DEBUG] FileName %v exists!\n", name)
	}
	return !exists
}


func (cr *ConfirmationRumorer) handleUnconfirmedTLCMessage(msg *TLCMessage) {
	fmt.Printf("UNCONFIRMED GOSSIP origin %v ID %v file name %v size %v metahash %v\n",
		msg.Origin, msg.ID, msg.TxBlock.Transaction.Name, msg.TxBlock.Transaction.Size, hex.EncodeToString(msg.TxBlock.Transaction.MetafileHash))

	valid := cr.checkFileName(msg.TxBlock.Transaction.Name)

	var sendAck bool
	if HW3EX3 {
		// Calculate the round of the peer
		peerRound := cr.roundOfPeer(msg.Origin, msg.ID)
		// Only send ack if peerRound >= my_time
		sendAck = peerRound >= cr.my_time && valid

		if peerRound < cr.my_time {
			fmt.Printf("[DEBUG] Received TLC from earlier round: my_time: %v, peerRound: %v, id: %v, origin: %v\n", cr.my_time, peerRound, msg.ID, msg.Origin)
		}

	} else if HW3EX2 {
		// For HW3EX2, always send ack if valid: there are no rounds
		sendAck = valid
	}

	if sendAck {
		ack := &TLCAck{
			Origin:      cr.name,
			ID:          msg.ID,
			Destination: msg.Origin,
			HopLimit:    cr.hopLimit,
		}

		fmt.Printf("SENDING ACK origin %v ID %v\n", msg.Origin, msg.ID)

		cr.p2pOut <- &AddrGossipPacket{UDPAddr{}, &GossipPacket{Ack: ack}}
	}
}


func (cr *ConfirmationRumorer) handleConfirmedTLCMessage(tlc *TLCMessage) {
	// TODO: print id or confirmed??  -> Confirmed for the moment...

	fmt.Printf("CONFIRMED GOSSIP origin %v ID %v file name %v size %v metahash %v\n",
		tlc.Origin, tlc.Confirmed, tlc.TxBlock.Transaction.Name, tlc.TxBlock.Transaction.Size, hex.EncodeToString(tlc.TxBlock.Transaction.MetafileHash))

	cr.confirmedGossipsMutex.Lock()
	cr.confirmedGossips = append(cr.confirmedGossips, ConfirmedGossipInfo{
		Origin:   tlc.Origin,
		ID:       tlc.Confirmed,
		Size:     tlc.TxBlock.Transaction.Size,
		Filename: tlc.TxBlock.Transaction.Name,
		Meta:      tlc.TxBlock.Transaction.MetafileHash,
	})
	cr.confirmedGossipsMutex.Unlock()

	roundPeer := cr.roundOfPeer(tlc.Origin, uint32(tlc.Confirmed))
	if Debug {
		fmt.Printf("[DEBUG] ROUND PEER: %v, MY_TIME: %v\n", roundPeer, cr.my_time)
	}
	if HW3EX3 && roundPeer == cr.my_time {
		cr.markConfirmedPeer(tlc.Origin, tlc.ID)
	}

	cr.confirmFile(tlc.TxBlock.Transaction.Name, nil)
}


func (cr *ConfirmationRumorer) markConfirmedPeer(origin string, id uint32) {
	cr.confirmedPeersMutex.Lock()
	if Debug {
		fmt.Printf("[DEBUG] Checking if we have majority, len=%v\n", len(cr.confirmedPeers))
	}
	if len(cr.confirmedPeers) <= cr.N/2 {
		cr.confirmedPeers[origin] = id
	} else {
		// Only notify once on gotMajority channel!
		cr.confirmedPeers[origin] = id
		if len(cr.confirmedPeers) > cr.N/2 {
			cr.gotMajority <- true
		}
	}

	cr.confirmedPeersMutex.Unlock()
}


func (cr *ConfirmationRumorer) handleAck(from UDPAddr, ack *TLCAck) {
	cr.collectedAcksMutex.RLock()
	defer cr.collectedAcksMutex.RUnlock()

	if ackChan, exists := cr.collectedAcks[ack.ID]; exists {
		// recover from panic caused by writing to a closed channel
		// by simply ignoring the ack: we already have enough
		defer func() {
			if r := recover(); r != nil {
				return
			}
		}()
		ackChan <- ack
	}
}


func (cr *ConfirmationRumorer) roundOfPeer(origin string, id uint32) int {
	// Get the round in which a peer is, based on the received TLC's from the peer
	// Also updates the known TLC's from the peer

	cr.peerRoundsMutex.Lock()
	_, exists := cr.peerRounds[origin]
	if !exists {
		cr.peerRounds[origin] = make(map[uint32]bool)
	}
	cr.peerRounds[origin][id] = true
	peerRound := len(cr.peerRounds[origin])

	cr.peerRoundsMutex.Unlock()

	return peerRound - 1
}


func (cr *ConfirmationRumorer) createTLCMessageWithReply(file *File, id uint32, confirmed int, replyChan chan uint32) *TLCMessageWithReplyChan {
	var prevHash [32]byte  // 0 for now
	return &TLCMessageWithReplyChan{
		Msg: &TLCMessage{
			Origin:      cr.name,
			ID:          id,
			Confirmed:   confirmed,
			TxBlock:     BlockPublish{
				PrevHash:    prevHash,
				Transaction: TxPublish{
					Name:         file.Name,
					Size:         file.Size,
					MetafileHash: file.Hash[:],
				},
			},
			VectorClock: nil,
			Fitness:     0,
		},
		ReplyChan: replyChan,
	}
}
