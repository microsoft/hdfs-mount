package main

import (
	"io"
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
}

var _ io.ReaderAt = (*RandomAccessHdfsReader)(nil) // ensure RandomAccessHdfsReader implements io.ReaderAt
var _ io.Closer = (*RandomAccessHdfsReader)(nil)   // ensure RandomAccessHdfsReader implements io.Closer

func NewRandomAccessHdfsReader(hdfsAccessor HdfsAccessor, path string) (*RandomAccessHdfsReader, error) {
	this := &RandomAccessHdfsReader{HdfsAccessor: hdfsAccessor, Path: path}
	attrs, err := hdfsAccessor.Stat(path)
	if err != nil {
		return nil, err
	}
	this.Size = attrs.Size
	return this, nil
}

func (this *RandomAccessHdfsReader) ReadAt(buffer []byte, offset int64) (int, error) {
	return 0, nil
}

func (this *RandomAccessHdfsReader) Close() error {
	return nil
}
