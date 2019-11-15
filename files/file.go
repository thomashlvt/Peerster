package files

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"sync"
)

const CHUNKSIZE = 8192

// Wrapper struct to abstract all file manipulations
type File struct {
	path   string

	size int64
	meta [][32]byte
	hash [32]byte
}

func NewFile(path string) *File {
	return &File{
		path:   path,
	}
}

func (f *File) getChunk(handle *os.File) []byte {
	// Returns CHUNK_SIZE bytes from the file (or less if it is the last chunk)

	// Create and fill buffer
	buf := make([]byte, CHUNKSIZE)
	num, err := handle.Read(buf)

	if err != nil {
		log.Fatalf("Could not read from file '%s': %s\n", f.path, err)
	}
	return buf[:num]
}

func (f *File) CreateMeta(chunkStorage *map[[32]byte][]byte, chunkStorageMutex *sync.RWMutex) {
	// Open the file
	handle, err := os.Open(f.path)
	if err != nil {
		log.Fatalf("Could not open file '%s': %s\n", f.path, err)
	}

	// Get file size
	info, err := handle.Stat()
	if err != nil {
		log.Fatalf("Could not read file info '%s': %s\n", f.path, err)
	}
	f.size = info.Size()

	// Calculate number of chunks
	var remainder int64 = 0
	if f.size%CHUNKSIZE != 0 {
		remainder = 1
	}
	numChunks := int(f.size/CHUNKSIZE + remainder)

	// Init META file
	f.meta = make([][32]byte, numChunks)

	for i := 0; i < numChunks; i += 1 {
		// Calculate hash of a chunk and put it in META file
		chunk := f.getChunk(handle)
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
	f.hash = sha256.Sum256(bytes)

	// Store the metafile
	chunkStorageMutex.Lock()
	(*chunkStorage)[f.hash] = bytes
	chunkStorageMutex.Unlock()

	err = handle.Close()
	if err != nil { log.Fatalf("Could not close file '%s': %s\n", f.path, err) }

	fmt.Printf("METAFILE %s\n", hex.EncodeToString(f.hash[:]))
}
