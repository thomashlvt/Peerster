package files

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	. "github.com/thomashlvt/Peerster/constants"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	"io/ioutil"
	"log"
	"path"
	"sync"
	"time"
)

const TIMEOUT = 30

type chanId struct {
	origin string
	hash   [32]byte
}

type SearchDownload struct {
	ReplyChan chan bool
	Hash [32]byte
	Destination *string
	Peers [] string
	Name string
}

type FileWithReplyChan struct {
	File *File
	ReplyChan chan bool
}

type FileHandler struct {
	name string

	in   chan *AddrGossipPacket
	uiIn chan *Message

	pointToPointOut chan *AddrGossipPacket

	searchDownloadIn chan *SearchDownload

	confFileOut chan *FileWithReplyChan

	downloadsInProgress      map[chanId]chan *DataReply
	downloadsInProgressMutex *sync.RWMutex

	// Store chunks separately so they can be found more efficiently
	chunks      map[[32]byte][]byte
	chunksMutex *sync.RWMutex

	files map[[32]byte] *File
	filesMutex *sync.RWMutex

	hopLimit uint32
}

func NewFileHandler(name string, in chan *AddrGossipPacket, uiIn chan *Message, ptpOut chan *AddrGossipPacket, hopLimit int) *FileHandler {
	return &FileHandler{
		name:                     name,
		in:                       in,
		uiIn:                     uiIn,
		pointToPointOut:          ptpOut,
		confFileOut:              make(chan *FileWithReplyChan),
		downloadsInProgress:      make(map[chanId]chan *DataReply),
		downloadsInProgressMutex: &sync.RWMutex{},
		searchDownloadIn:         make(chan *SearchDownload),
		chunks:                   make(map[[32]byte][]byte),
		chunksMutex:              &sync.RWMutex{},
		files:					  make(map[[32]byte]*File),
		filesMutex:               &sync.RWMutex{},
		hopLimit:                 uint32(hopLimit),
	}
}

func (fh *FileHandler) UIIn() chan *Message {
	return fh.uiIn
}

func (fh *FileHandler) Files() (*map[[32]byte]*File, *sync.RWMutex) {
	return &fh.files, fh.filesMutex
}

func (fh *FileHandler) SearchDownloadIn() chan *SearchDownload {
	return fh.searchDownloadIn
}

func (fh *FileHandler) ConfFileOut() chan *FileWithReplyChan {
	return fh.confFileOut
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
						if Debug {
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
								if Debug {
									fmt.Printf("[DEBUG] !! Hash did not check out !! Is data empty? %v\n",
										len(msg.Gossip.DataReply.Data) == 0)
								}
							} // Skip if hash not ok
						} else {
							if Debug {
								fmt.Printf("[DEBUG] Received data reply from %v outside of download!\n",
									msg.Gossip.DataReply.Origin)
							}
							fh.downloadsInProgressMutex.RUnlock()
						}

					} else if msg.Gossip.DataRequest != nil {
						if Debug {
							fmt.Printf("[DEBUG] Handling data request from %s\n", msg.Gossip.DataRequest.Origin)
						}
						go fh.handleDataRequest(msg.Gossip.DataRequest, msg.Address)
					} // ignore others
				}()
			case req := <- fh.searchDownloadIn:
				go func() {
					if req.Peers == nil {
						fh.downloadMetaFile(req.Name, req.Hash, *req.Destination)
						req.ReplyChan <- true
					} else {
						fh.downloadChunks(req.Name, req.Hash, req.Peers)
						req.ReplyChan <- true
					}
				}()
			}
		}
	}()
}

func (fh *FileHandler) handleNewFile(name string) {
	f := NewFile(name)

	// Chunk storage is passed to store the chunks
	chunks, hashes := f.LoadFromFileSystem()

	// Check name of new file
	var confirmed bool
	if HW3EX2 || HW3EX3 {
		replyChan := make(chan bool)
		fh.confFileOut <- &FileWithReplyChan{f, replyChan}
		confirmed = <- replyChan
	} else {
		confirmed = true
	}

	if confirmed {
		fh.addToStorage(chunks, hashes)
		// Add chunk to files map
		fh.filesMutex.Lock()
		fh.files[f.Hash] = f
		fh.filesMutex.Unlock()

		if HW2 {
			fmt.Printf("METAFILE %s\n", hex.EncodeToString(f.Hash[:]))
		}

		if Debug && HW2 {
			fmt.Printf("[DEBUG] Filename: %v\n", f.Name)
		}

	} else {
		if Debug {
			fmt.Printf("[DEBUG] Filename %v was not confirmed!\n", name)
		}
	}

}

func (fh *FileHandler) addToStorage(chunks [][]byte, hashes[][32]byte) {
	fh.chunksMutex.Lock()
	defer fh.chunksMutex.Unlock()

	for i, hash := range hashes {
		fh.chunks[hash] = chunks[i]
	}
}

func (fh *FileHandler) handleDataRequest(req *DataRequest, from UDPAddr) {
	repl := &DataReply{
		Origin:      fh.name,
		Destination: req.Origin,
		HopLimit:    fh.hopLimit,
		HashValue:   req.HashValue,
		Data:        fh.getChunk(To32Byte(req.HashValue)),
	}

	fh.pointToPointOut <- &AddrGossipPacket{UDPAddr{}, repl.ToGossip()}
}

func (fh *FileHandler) downloadFile(name string, hash [32]byte, peer string) {
	meta := fh.downloadMetaFile(name, hash, peer)
	// Download all chunks from the same peer
	peers := make([]string, len(meta) / 32)
	for i := 0; i < len(meta) / 32; i++ {
		peers[i] = peer
	}
	fh.downloadChunks(name, hash, peers)
}

func (fh *FileHandler) downloadMetaFile(name string, hash [32]byte, peer string) []byte {
	f := NewFile(name)
	if HW2 {
		fmt.Printf("DOWNLOADING metafile of %s from %s\n", name, peer)
	}
	meta := fh.downloadChunk(hash, peer)
	if Debug {
		fmt.Printf("[DEBUG] got metadata file for %s\n", hex.EncodeToString(hash[:]))
	}

	// Store metafile chunk
	fh.chunksMutex.Lock()
	fh.chunks[hash] = meta
	fh.chunksMutex.Unlock()

	// Add metadata to file
	f.loadMeta(meta, hash)

	// Add file to 'known' files: this should only be done when we have the metadata
	fh.filesMutex.Lock()
	fh.files[hash] = f
	fh.filesMutex.Unlock()

	return meta
}


func (fh *FileHandler) downloadChunks(name string, hash [32]byte, peers []string) {

	fmt.Printf("Downloading chunks\n")
	fh.filesMutex.RLock()
	f, exists := fh.files[hash]
	if !exists {
		if Debug {
			fmt.Printf("[DEBUG] Warning: metafile %v not found, skipping...\n", hex.EncodeToString(hash[:]))
		}
		fh.filesMutex.RUnlock()
		return
	}

	fh.filesMutex.RUnlock()

	// Download each data chunk serially
	chunks := make([]byte, 0)
	for i := 0; i < int(f.NumChunks); i += 1 {
		chunkHash := f.meta[i]
		if HW2 {
			fmt.Printf("DOWNLOADING %s chunk %d from %s\n", name, i, peers[i])
		}
		chunk := fh.downloadChunk(chunkHash, peers[i])
		chunks = append(chunks, chunk...)

		fh.chunksMutex.Lock()
		fh.chunks[chunkHash] = chunk
		fh.chunksMutex.Unlock()

		// Mark chunk as 'known'
		f.ChunkMap = append(f.ChunkMap, uint64(i+1))
	}

	// Write download to file
	err := ioutil.WriteFile(path.Join("_Downloads", name), chunks, 0755)
	if err != nil {
		log.Fatalf("Could not write download to file: %s\n", err)
	}
	if HW2 {
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
	 fh.downloadsInProgress[chanId{from,
	 	hash}] = replyChan
	fh.downloadsInProgressMutex.Unlock()

	req := &DataRequest{
		Origin:      fh.name,
		Destination: from,
		HopLimit:    fh.hopLimit,
		HashValue:   hash[:],
	}

	for {
		fh.pointToPointOut <- &AddrGossipPacket{UDPAddr{}, req.ToGossip()}

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
		if Debug {
			fmt.Printf("[DEBUG] Chunk requested that we don't have!\n")
		}
		return nil
	} else {
		return res
	}
}


func GetFilesFromFilesystem() []string {
	// Helper function to list files in _SharedFiles directory

	files, err := ioutil.ReadDir("_SharedFiles")
	if err != nil {
		log.Fatal(err)
	}

	res := make([]string, len(files))
	for i, f := range files {
		if !f.IsDir() {
			res[i] = f.Name()
		}
	}

	return res
}