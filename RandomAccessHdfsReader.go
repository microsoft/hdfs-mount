package main

import (
	"errors"
	"io"
	"log"
	"sync"
)

// Implments io.ReaderAt, io.Closer providing efficient concurrent random access to the file
// on HDFS. Concurrency is achieved by pooling HdfsReader objects. In order to optimize
// sequential read scenario of a fragment of the file, pool datastructure is organized
// as a dictionary keyed by the seek position, so continuation of reading of a chunk
// with high probability goes to the HdfsReader which is already at desired position with
// more data waiting in network buffers
type RandomAccessHdfsReader struct {
	HdfsAccessor HdfsAccessor         // HDFS accessor used to create HdfsReader objects
	Path         string               // Path to the file
	Size         uint64               // Cached size of the file
	Pool         map[int64]HdfsReader // Pool of HdfsReader objects keyed by the seek position
	PoolLock     sync.Mutex           // Exclusive lock for the Pool
	MaxReaders   int                  // Maximum number of readers in the pool
}

var _ io.ReaderAt = (*RandomAccessHdfsReader)(nil) // ensure RandomAccessHdfsReader implements io.ReaderAt
var _ io.Closer = (*RandomAccessHdfsReader)(nil)   // ensure RandomAccessHdfsReader implements io.Closer

func NewRandomAccessHdfsReader(hdfsAccessor HdfsAccessor, path string) (*RandomAccessHdfsReader, error) {
	this := &RandomAccessHdfsReader{
		HdfsAccessor: hdfsAccessor,
		Path:         path,
		Pool:         map[int64]HdfsReader{},
		MaxReaders:   100}
	attrs, err := hdfsAccessor.Stat(path)
	if err != nil {
		return nil, err
	}
	this.Size = attrs.Size
	return this, nil
}

func (this *RandomAccessHdfsReader) ReadAt(buffer []byte, offset int64) (int, error) {
	reader, err := this.getReaderFromPool(offset)
	defer func() {
		if err == nil {
			this.returnReaderToPool(reader)
		} else {
			if reader != nil {
				go reader.Close()
			}
		}
	}()
	if err != nil {
		return 0, err
	}
	readerPos, err := reader.Position()
	if err != nil {
		return 0, err
	}
	if readerPos != offset {
		err := reader.Seek(offset)
		if err != nil {
			return 0, err
		}
	}
	nr, err := reader.Read(buffer)
	return nr, err
}

// Closes all the readers
func (this *RandomAccessHdfsReader) Close() error {
	this.PoolLock.Lock()
	defer this.PoolLock.Unlock()
	log.Printf("RandomAccessHdfsReader[%s]: closing %d readers\n", this.Path, len(this.Pool))
	for _, reader := range this.Pool {
		reader.Close()
	}
	this.Pool = nil
	return nil
}

// Retrievs an optimal reader from pool or create new one
func (this *RandomAccessHdfsReader) getReaderFromPool(offset int64) (HdfsReader, error) {
	this.PoolLock.Lock()
	if this.Pool == nil {
		this.PoolLock.Unlock()
		return nil, errors.New("RandomAccessHdfsReader closed")
	}
	if len(this.Pool) == 0 {
		// Empty pool. Creating new reader and returning it directly
		this.PoolLock.Unlock()
		return this.HdfsAccessor.OpenRead(this.Path)
	}
	reader, ok := this.Pool[offset]
	var key int64
	if ok {
		// Found perfect reader
		key = offset
	} else {
		// Take a random reader from the map
		// Note: go randomizes map enumeration, so we're leveraging it here
		for k, v := range this.Pool {
			key = k
			reader = v
			break
		}
	}
	// removing from pool before returning
	delete(this.Pool, key)
	this.PoolLock.Unlock()
	return reader, nil
}

// Returns idle reader back to the pool
func (this *RandomAccessHdfsReader) returnReaderToPool(reader HdfsReader) {
	this.PoolLock.Lock()
	defer this.PoolLock.Unlock()
	// If pool was destroyed or is full then closing current reader w/o returning
	if this.Pool == nil || len(this.Pool) >= this.MaxReaders {
		go reader.Close()
		return
	}

	// Getting reader position, if failed - we can't return readed to the pool
	key, err := reader.Position()
	if err != nil {
		go reader.Close()
		return
	}

	prevReader, ok := this.Pool[key]
	if ok {
		// We had other reader at the same position,
		// closing that one
		go prevReader.Close()
	}

	// Returning reader to the pool
	this.Pool[key] = reader
}
