package search

import (
	"encoding/hex"
	"fmt"
	. "github.com/thomashlvt/Peerster/constants"
	. "github.com/thomashlvt/Peerster/files"
	. "github.com/thomashlvt/Peerster/udp"
	. "github.com/thomashlvt/Peerster/utils"
	"math/rand"
	"strings"
	"sync"
	"time"
)

const FLOODTIMEOUT = 1
const MATCHTHRESHOLD = 2
const BUDGETTHRESHOLD = 32

// TODO: check if search request on own node
// TODO: currently is also processed by own nodes

type Searcher struct {
	in      chan *AddrGossipPacket
	out     chan *AddrGossipPacket
	uiIn    chan *Message
	p2pOut  chan *AddrGossipPacket

	searchDownloadOut chan *SearchDownload

	searchReplies chan *SearchReply

	name  string

	peers *Set
	requestCache *SearchRequestCache

	files *map[[32]byte]*File
	filesMutex *sync.RWMutex

	knownFiles map[[32]byte] []*string
	knownFilesMutex *sync.RWMutex
}

func NewSearcher(name string, peers *Set, in chan *AddrGossipPacket, out chan *AddrGossipPacket,
	uiIn chan *Message, p2pOut chan *AddrGossipPacket, sDOut chan *SearchDownload, files *map[[32]byte]*File, filesMutex *sync.RWMutex) *Searcher {
	cache := NewSearchRequestCache()
	cache.Run()
	return &Searcher{
		in: in,
		out: out,
		uiIn: uiIn,
		p2pOut: p2pOut,
		searchDownloadOut: sDOut,
		searchReplies: make(chan *SearchReply),
		name: name,
		peers: peers,
		requestCache: cache,
		files: files,
		filesMutex: filesMutex,
		knownFiles: make(map[[32]byte] []*string),
		knownFilesMutex: &sync.RWMutex{},
	}
}

func (s *Searcher) Run() {
	go func() {
		for {
			select{
			case msg := <- s.uiIn:
				go s.handleUIMessage(msg)
			case packet := <- s.in:
				if packet.Gossip.SearchRequest != nil {
					go s.handleSearchRequest(packet.Gossip.SearchRequest, packet.Address)
				} else if packet.Gossip.SearchReply != nil {
					// Assume that no other search queries are happening in parallel
					s.searchReplies <- packet.Gossip.SearchReply
				}
			}
		}
	}()
}

type fileID struct {meta [32]byte; fileName string}


func (s *Searcher) search(keywords []string, budget *uint64) {
	// Keep a map to efficiently check if we got a duplicate result
	completeFiles := make(map[fileID]*SearchResult, 0)
	waitGroup := &sync.WaitGroup{}

	// Look for matches locally
	matches := s.findMatch(keywords)
	for _, match := range matches {
		completeFiles[fileID{To32Byte(match.MetafileHash), match.FileName}] = match
	}

	if budget != nil {
		if len(completeFiles) < MATCHTHRESHOLD {
			// TODO: should we keep on retrying here?
			req := SearchRequest{
				Origin:   s.name,
				Budget:   *budget - 1,  // -1 because we checked the local node
				Keywords: keywords,
			}
			s.searchIteration(&req, completeFiles, waitGroup)
		}

	} else {
		currBudget := 2
		for currBudget <= BUDGETTHRESHOLD && len(completeFiles) < MATCHTHRESHOLD{
			req := SearchRequest{
				Origin:   s.name,
				Budget:   uint64(currBudget - 1),  // -1 because we checked the local node
				Keywords: keywords,
			}
			s.searchIteration(&req, completeFiles, waitGroup)
			currBudget *= 2
		}
	}
	waitGroup.Wait()

	if len(completeFiles) >= MATCHTHRESHOLD {
		fmt.Printf("SEARCH FINISHED\n")
	} else if Debug {
		fmt.Printf("[DEBUG] Search finished but no matches found\n")
	}
}


func (s *Searcher) searchIteration(req *SearchRequest, completeFiles map[fileID]*SearchResult, waitGroup *sync.WaitGroup) {
	s.flood(req)
	timer := time.NewTicker(time.Second * FLOODTIMEOUT)

	done := false
	for !done {
		select {
		case <- timer.C:
			done = true
		case reply := <- s.searchReplies:
			results := s.processSearchReply(reply)

			// Add results to complete files
			for _, result := range results {
				meta := To32Byte(result.MetafileHash)
				if _, exists := completeFiles[fileID{meta, result.FileName}]; !exists {
					completeFiles[fileID{meta, result.FileName}] = result
				}
			}

			// Process results
			for _, result := range results {
				waitGroup.Add(1)
				go s.processCompleteFile(result, reply.Origin, waitGroup)
			}

			if len(completeFiles) >= MATCHTHRESHOLD {
				done = true
			}
		}
	}
}


func (s *Searcher) download(fileName string, request [32]byte) {
	s.knownFilesMutex.RLock()
	peers := s.knownFiles[request]
	peersComplete := make([]string, len(peers))
	for i, peer := range peers {
		if peer != nil {
			peersComplete[i] = *peer
		} else {
			if Debug {
				fmt.Println("[DEBUG] Warning: file you are trying to download is not complete, skipping...")
			}
			s.knownFilesMutex.RUnlock()
			return
		}
	}
	s.knownFilesMutex.RUnlock()

	replyChan := make(chan bool)
	req := &SearchDownload{
		ReplyChan:   replyChan,
		Hash:        request,
		Peers:       peersComplete,
		Name:        fileName,
	}

	s.searchDownloadOut <- req

	// <- replyChan TODO: wait on reply?

}

func (s *Searcher) handleUIMessage(msg *Message) {
	if msg.Keywords != nil {
		if Debug {
			fmt.Printf("[DEBUG] Got UI search msg for: %v\n", *msg.Keywords)
		}
		s.search(*msg.Keywords, msg.Budget)
	} else {
		if Debug {
			fmt.Printf("[DEBUG] Got UI download msg for file: %v\n", *msg.File)
		}
		s.download(*msg.File, To32Byte(*msg.Request))
	}
}

func (s *Searcher) processSearchReply(reply *SearchReply) []*SearchResult {
	s.knownFilesMutex.Lock()
	defer s.knownFilesMutex.Unlock()

	completeFiles := make([]*SearchResult, 0)
	for _, result := range reply.Results {
		_, exists := s.knownFiles[To32Byte(result.MetafileHash)]
		if !exists {
			s.knownFiles[To32Byte(result.MetafileHash)] = make([]*string, result.ChunkCount)
		}

		for _, chunk := range result.ChunkMap {
			s.knownFiles[To32Byte(result.MetafileHash)][int(chunk-1)] = &reply.Origin
		}

		if s.checkComplete(s.knownFiles[To32Byte(result.MetafileHash)]) {
			completeFiles = append(completeFiles, result)
		}
	}
	return completeFiles
}

func (s *Searcher) processCompleteFile(result *SearchResult, peer string, group *sync.WaitGroup) {
	defer group.Done()

	chunksStr := ""
	for _, chunk := range result.ChunkMap {
		chunksStr += fmt.Sprintf("%v", chunk)
		chunksStr += ","
	}
	if len(chunksStr) > 0 {
		chunksStr = chunksStr[:len(chunksStr)-1]
	}

	toPrint := 	fmt.Sprintf("FOUND match %v at %v\n", result.FileName, peer) +
		fmt.Sprintf("metafile=%v chunks=%v\n", hex.EncodeToString(result.MetafileHash), chunksStr)
	fmt.Printf(toPrint)



	s.knownFilesMutex.RLock()
	metaPeers := s.knownFiles[To32Byte(result.MetafileHash)]
	metaPeer := metaPeers[rand.Int() % len(metaPeers)]
	replyChan := make(chan bool)
	req := &SearchDownload{
		ReplyChan:   replyChan,
		Hash:        To32Byte(result.MetafileHash),
		Destination: metaPeer,
		Name:        result.FileName,
	}
	s.knownFilesMutex.RUnlock()
	s.searchDownloadOut <- req

	// TODO: what if metafile eventually not found?

	<- replyChan // TODO: wait for reply?
}

func (s *Searcher) handleSearchRequest(req *SearchRequest, addr UDPAddr) {
	if Debug {
		fmt.Printf("[DEBUG] Got search request for: %v, from: %v, budget: %v\n", req.Keywords, req.Origin, req.Budget)
	}

	// Check if duplicate request
	duplicate := s.requestCache.Check(req.Origin, req.Keywords)
	if duplicate {
		if Debug {
			fmt.Printf("[DEBUG] Duplicate search request from: %v, for %v\n", req.Origin, req.Keywords)
		}
		return
	}

	// Check for a match locally
	matches := s.findMatch(req.Keywords)
	if len(matches) > 0 {
		if Debug {
			fmt.Printf("[DEBUG] Matches found for: %v\n", req.Keywords)
		}
		// Handle match
		reply := SearchReply{
			Origin:      s.name,
			Destination: req.Origin,
			HopLimit:    HOPLIMIT,
			Results:     matches,
		}
		s.p2pOut <- &AddrGossipPacket{UDPAddr{}, &GossipPacket{SearchReply: &reply}}
		
	} else {
		// No match: flood the request
		if Debug {
			fmt.Printf("[DEBUG] No matches found for: %v\n", req.Keywords)
		}
		if req.Budget > 1 {
			req.Budget -= 1
			s.flood(req)
		} else {
			if Debug {
				fmt.Printf("[DEBUG] No more budget for: %v, from %v\n", req.Keywords, req.Origin)
			}
		}
	}
}

func (s *Searcher) flood(req *SearchRequest) {
	// TODO shuffle peers
	minBudgetPerPeer := int(req.Budget) / s.peers.Len()
	extraBudget := int(req.Budget) % s.peers.Len()
	if Debug {
		fmt.Println("[DEBUG] Flooding...")
	}
	for _, peer := range s.peers.Data() {
		budget := minBudgetPerPeer
		if extraBudget > 0 {
			budget += 1
			extraBudget -= 1
		}
		if Debug {
			fmt.Printf("[DEBUG] Sending SearchRequest to %v\n", peer)
		}
		s.out <- &AddrGossipPacket{peer, &GossipPacket{SearchRequest: req}}
	}
}

func (s *Searcher) findMatch(keywords []string) []*SearchResult {
	res := make([]*SearchResult, 0)
	for _, file := range *s.files {
		for _, keyword := range keywords {
			if strings.Contains(file.Name, keyword) {
				sr := SearchResult{
					FileName:     file.Name,
					MetafileHash: file.Hash[:],
					ChunkMap:     file.ChunkMap,
					ChunkCount:   file.NumChunks,
				}
				res = append(res, &sr)
				break
			}
		}
	}
	return res
}


func (s *Searcher) checkComplete(peers []*string) bool {
	// Check if there is a peer for every chunk in the peers list

	for _, peer := range peers {
		if peer == nil {
			return false
		}
	}
	return true
}

