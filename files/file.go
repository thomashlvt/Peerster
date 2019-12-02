package files

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	. "github.com/thomashlvt/Peerster/utils"
	"log"
	"os"
	"path"
	"sync"

	. "github.com/thomashlvt/Peerster/constants"
)

const CHUNKSIZE = 8192

// TODO: sort chunkmap?

// Wrapper struct to abstract all file manipulations
type File struct {
	Name   string

	meta [][32]byte
	Hash [32]byte

	ChunkMap []uint64
	NumChunks uint64
}

func NewFile(fileName string) *File {
	return &File{
		Name:   fileName,
		ChunkMap:   make([]uint64, 0),
	}
}

func (f *File) LoadFromFileSystem(chunkStorage *map[[32]byte][]byte, chunkStorageMutex *sync.RWMutex) {
	// Open the file
	handle, err := os.Open(path.Join("_SharedFiles", f.Name))
	if err != nil {
		// TODO: should not crash!
		log.Fatalf("Could not open file '%s': %s\n", f.Name, err)
	}

	// Get file size
	info, err := handle.Stat()
	if err != nil {
		log.Fatalf("Could not read file info '%s': %s\n", f.Name, err)
	}
	size := info.Size()

	// Calculate number of chunks
	var remainder int64 = 0
	if size%CHUNKSIZE != 0 {
		remainder = 1
	}
	f.NumChunks = uint64(size/CHUNKSIZE + remainder)

	// Init META file
	f.meta = make([][32]byte, int(f.NumChunks))

	for i := 0; i < int(f.NumChunks); i += 1 {
		// Calculate hash of a chunk and put it in META file
		// Get the chunk
		buf := make([]byte, CHUNKSIZE)
		num, err := handle.Read(buf)

		if err != nil {
			log.Fatalf("Could not read from file '%s': %s\n", f.Name, err)
		}
		chunk := buf[:num]
		hash := sha256.Sum256(chunk)
		f.meta[i] = hash

		// Store the chunks
		chunkStorageMutex.Lock()
		(*chunkStorage)[hash] = chunk
		chunkStorageMutex.Unlock()
	}

	// Calculate hash of hashes
	bytes := make([]byte, 0)
	for _, hash := range f.meta {
		bytes = append(bytes, hash[:]...)
	}
	f.Hash = sha256.Sum256(bytes)

	// Store the metafile
	chunkStorageMutex.Lock()
	(*chunkStorage)[f.Hash] = bytes
	chunkStorageMutex.Unlock()

	err = handle.Close()
	if err != nil { log.Fatalf("Could not close file '%s': %s\n", f.Name, err) }

	if HW2 {
		fmt.Printf("METAFILE %s\n", hex.EncodeToString(f.Hash[:]))
	}

	if Debug && HW2 {
		fmt.Printf("[DEBUG] Filename: %v\n", f.Name)
	}

	f.ChunkMap = make([]uint64, f.NumChunks)
	for i := 0; i < int(f.NumChunks); i++ {
		f.ChunkMap[i] = uint64(i+1)
	}
}

func (f *File) loadMeta(meta []byte, hash [32]byte) {
	f.NumChunks = uint64(len(meta) / 32)
	f.meta = make([][32]byte, f.NumChunks)
	for i := 0; i < int(f.NumChunks); i++ {
		f.meta[i] = To32Byte(meta[i * 32: (i+1) * 32])
	}
	f.Hash = hash
}
