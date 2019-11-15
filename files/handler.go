package files

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	"io/ioutil"
	"log"
	"path"
	"sync"
	"time"
)

const HOPLIMIT = 10
const TIMEOUT = 30

type chanId struct {
	origin string
	hash   [32]byte
}


type FileHandler struct {
	name string

	in   chan *AddrGossipPacket
	uiIn chan *Message

	pointToPointOut chan *AddrGossipPacket

	downloadsInProgress      map[chanId]chan *DataReply
	downloadsInProgressMutex *sync.RWMutex

	chunks      map[[32]byte][]byte
	chunksMutex *sync.RWMutex

	debug bool
	hw1   bool
	hw2   bool
}

func NewFileHandler(name string, in chan *AddrGossipPacket, uiIn chan *Message, ptpOut chan *AddrGossipPacket, debug bool, hw1 bool, hw2 bool) *FileHandler {
	return &FileHandler{
		name:                     name,
		in:                       in,
		uiIn:                     uiIn,
		pointToPointOut:          ptpOut,
		downloadsInProgress:      make(map[chanId]chan *DataReply),
		downloadsInProgressMutex: &sync.RWMutex{},
		chunks:                   make(map[[32]byte][]byte),
		chunksMutex:              &sync.RWMutex{},
		debug:                    debug,
		hw1:                      hw1,
		hw2:                      hw2,
	}
}

func (fh *FileHandler) UIIn() chan *Message {
	return fh.uiIn
}

func (fh *FileHandler) Run() {
	go func() {
		for {
			select {
			case msg := <-fh.uiIn:
				if msg.Request != nil {
					// This is a file request
					go fh.downloadFile(*msg.File, To32Byte(*msg.Request), *msg.Destination)
				} else {
					// This is a file upload
					go fh.handleNewFile(*msg.File)
				}

			case msg := <-fh.in:
				go func(){
					if msg.Gossip.DataReply != nil {
						if fh.debug {
							fmt.Printf("[DEBUG] Dispatching data reply from %s\n", msg.Gossip.DataReply.Origin)
						}
						// Dispatch reply to downloads in progress
						hash := To32Byte(msg.Gossip.DataReply.HashValue)
						fh.downloadsInProgressMutex.RLock()
						c, ok := fh.downloadsInProgress[chanId{msg.Gossip.DataReply.Origin, hash}]
						if ok {
							if hash == sha256.Sum256(msg.Gossip.DataReply.Data) {
								c <- msg.Gossip.DataReply
								fh.downloadsInProgressMutex.RUnlock()

							} else {
								if fh.debug {
									fmt.Printf("[DEBUG] !! Hash did not check out !! Is data empty? %v\n",
										len(msg.Gossip.DataReply.Data) == 0)
								}
							} // Skip if hash not ok
						} else {
							if fh.debug {
								fmt.Printf("[DEBUG] Received data reply from %v outside of download!\n",
									msg.Gossip.DataReply.Origin)
							}
							fh.downloadsInProgressMutex.RUnlock()
						}

					} else if msg.Gossip.DataRequest != nil {
						if fh.debug {
							fmt.Printf("[DEBUG] Handling data request from %s\n", msg.Gossip.DataRequest.Origin)
						}
						go fh.handleDataRequest(msg.Gossip.DataRequest, msg.Address)
					} // ignore others
				}()
			}
		}
	}()
}

func (fh *FileHandler) handleNewFile(name string) {
	fPath := path.Join("_SharedFiles", name)
	f := NewFile(fPath)

	// Chunk storage is passed to store the chunks
	f.CreateMeta(&fh.chunks, fh.chunksMutex)
}

func (fh *FileHandler) handleDataRequest(req *DataRequest, from UDPAddr) {
	repl := &DataReply{
		Origin:      fh.name,
		Destination: req.Origin,
		HopLimit:    HOPLIMIT,
		HashValue:   req.HashValue,
		Data:        fh.getChunk(To32Byte(req.HashValue)),
	}

	fh.pointToPointOut <- &AddrGossipPacket{UDPAddr{}, repl.ToGossip()}
}

func (fh *FileHandler) downloadFile(name string, hash [32]byte, from string) {
	// 1. Request metafile
	if fh.hw2 {
		fmt.Printf("DOWNLOADING metafile of %s from %s\n", name, from)
	}
	meta := fh.downloadChunk(hash, from)
	if fh.debug {
		fmt.Printf("[DEBUG] got metadata file for %s\n", hex.EncodeToString(hash[:]))
	}

	//    Store metafile chunk
	fh.chunksMutex.Lock()
	fh.chunks[hash] = meta
	fh.chunksMutex.Unlock()

	// 2. Download each data chunk serially
	chunks := make([]byte, 0)
	for i := 0; i < len(meta)/32; i += 1 {
		chunkHash := To32Byte(meta[i*32 : (i+1)*32])
		if fh.hw2 {
			fmt.Printf("DOWNLOADING %s chunk %d from %s\n", name, i, from)
		}
		chunk := fh.downloadChunk(chunkHash, from)
		chunks = append(chunks, chunk...)

		fh.chunksMutex.Lock()
		fh.chunks[chunkHash] = chunk
		fh.chunksMutex.Unlock()
	}

	// 3. Write download to file
	err := ioutil.WriteFile(path.Join("_Downloads", name), chunks, 0755)
	if err != nil {
		log.Fatalf("Could not write download to file: %s\n", err)
	}
	if fh.hw2 {
		fmt.Printf("RECONSTRUCTED file %s\n", name)
	}
}

func (fh *FileHandler) downloadChunk(hash [32]byte, from string) []byte {
	// Listen for replies
	replyChan := make(chan *DataReply)
	fh.downloadsInProgressMutex.Lock()
	otherChan, exists := fh.downloadsInProgress[chanId{from, hash}]

	 // If exists there is a 2nd channel that is downloading the same chunk from the same peer
	 // We will now be listening, and will notify the other peer
	 fh.downloadsInProgress[chanId{from, hash}] = replyChan
	fh.downloadsInProgressMutex.Unlock()

	req := &DataRequest{
		Origin:      fh.name,
		Destination: from,
		HopLimit:    HOPLIMIT,
		HashValue:   hash[:],
	}

	for {
		fh.pointToPointOut <- &AddrGossipPacket{UDPAddr{}, req.ToGossip()}

		if fh.debug {
			fmt.Printf("[DEBUG] Requested metafile %s\n", hex.EncodeToString(hash[:]))
		}

		timer := time.NewTicker(TIMEOUT * time.Second)
		select {
		case <-timer.C:
			timer.Stop()
			// Retry:
			continue
		case reply := <-replyChan:
			// Reply came in:
			if exists {
				// Also notify the other peer
				otherChan <- reply
			}
			// Now delete our channel, if it is still in the map (not guaranteed!)
			fh.downloadsInProgressMutex.Lock()
			delete(fh.downloadsInProgress, chanId{from, hash})
			fh.downloadsInProgressMutex.Unlock()
			return reply.Data
		}
	}
}

func (fh *FileHandler) getChunk(hash [32]byte) []byte {
	fh.chunksMutex.RLock()
	defer fh.chunksMutex.RUnlock()
	res, ok := fh.chunks[hash]
	if !ok {
		if fh.debug {
			fmt.Print("[DEBUG] Chunk requested that we don't have!")
		}
		return nil
	} else {
		return res
	}
}


func (fh *FileHandler) GetFilesFromFilesystem() []string {
	// Helper method go get possible files to be shared
	files, err := ioutil.ReadDir("_SharedFiles")
	if err != nil {
		log.Fatal(err)
	}
	res := make([]string, 0)
	for _, f := range files {
		if !f.IsDir() {
			res = append(res, f.Name())
		}
	}
	return res
}