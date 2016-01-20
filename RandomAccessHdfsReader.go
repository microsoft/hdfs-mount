package main

import (
	"errors"
	"io"
	"log"
	"sync"
)

// RandomAccessHdfsReader Implments io.ReaderAt, io.Closer providing efficient concurrent
// random access to the file on HDFS. Concurrency is achieved by pooling ReadSeekCloser objects.
// In order to optimize sequential read scenario of a fragment of the file, pool datastructure
// is organized as a map keyed by the seek position, so sequential read of adjacent file chunks
// with high probability goes to the same ReadSeekCloser
type RandomAccessHdfsReader interface {
	io.ReaderAt
	io.Closer
}

type randomAccessHdfsReaderImpl struct {
	HdfsAccessor HdfsAccessor         // HDFS accessor used to create ReadSeekCloser objects
	Path         string               // Path to the file
	Pool         map[int64]ReadSeekCloser // Pool of ReadSeekCloser objects keyed by the seek position
	PoolLock     sync.Mutex           // Exclusive lock for the Pool
	MaxReaders   int                  // Maximum number of readers in the pool
}

var _ RandomAccessHdfsReader = (*randomAccessHdfsReaderImpl)(nil) // ensure randomAccessReadSeekCloser implements RandomAccessHdfsReader

func NewRandomAccessHdfsReader(hdfsAccessor HdfsAccessor, path string) RandomAccessHdfsReader {
	this := &randomAccessHdfsReaderImpl{
		HdfsAccessor: hdfsAccessor,
		Path:         path,
		Pool:         map[int64]ReadSeekCloser{},
		MaxReaders:   100}
	return this
}

func (this *randomAccessHdfsReaderImpl) ReadAt(buffer []byte, offset int64) (int, error) {
	reader, err := this.getReaderFromPoolOrCreateNew(offset)
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
	nr, err := io.ReadFull(reader, buffer)
	return nr, err
}

// Closes all the readers
func (this *randomAccessHdfsReaderImpl) Close() error {
	this.PoolLock.Lock()
	defer this.PoolLock.Unlock()
	log.Printf("RandomAccessHdfsReader[%s]: closing %d readers\n", this.Path, len(this.Pool))
	for _, reader := range this.Pool {
		reader.Close()
	}
	this.Pool = nil
	return nil
}

// Retrieves an optimal reader from pool or creates new one
func (this *randomAccessHdfsReaderImpl) getReaderFromPoolOrCreateNew(offset int64) (ReadSeekCloser, error) {
	reader, err := this.getReaderFromPool(offset)
	if err != nil {
		return reader, err
	}
	if reader != nil {
		return reader, nil
	} else {
		// Creating new reader
		return this.HdfsAccessor.OpenRead(this.Path)
	}
}

// Retrievs an optimal reader from pool or nil if pool is empty
func (this *randomAccessHdfsReaderImpl) getReaderFromPool(offset int64) (ReadSeekCloser, error) {
	this.PoolLock.Lock()
	defer this.PoolLock.Unlock()
	if this.Pool == nil {
		return nil, errors.New("RandomAccessHdfsReader closed")
	}
	if len(this.Pool) == 0 {
		// Empty pool
		return nil, nil
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
	return reader, nil
}

// Returns idle reader back to the pool
func (this *randomAccessHdfsReaderImpl) returnReaderToPool(reader ReadSeekCloser) {
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
