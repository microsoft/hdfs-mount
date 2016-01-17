// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
)

// Basic test for HdfsRandomAccessReader
func TestHdfsRandomAccessReader(t *testing.T) {
	// Setting up mockery, to serve that large virtual file
	fileSize := int64(5 * 1024 * 1024 * 1024)
	hdfsAccessor := &MockRandomAccessHdfsAccessor{}
	reader := NewRandomAccessHdfsReader(hdfsAccessor, "/path/to/5G.blob")
	// Launching 10 parallel goroutines to concurrently read fragments of a file
	var join sync.WaitGroup
	allSuccessful := true
	fmt.Printf("Forking into 10 goroutines\n")
	for i := 0; i < 10; i++ {
		join.Add(1)
		offset := int64(i) * fileSize / 11
		go func() {
			defer join.Done()
			// Performing 1000 sequential reads in each gorutine
			for j := 0; j < 1000; j++ {
				buffer := make([]byte, 4096)
				actualBytesRead, err := reader.ReadAt(buffer, offset)
				if err != nil {
					fmt.Printf("Error %v\n", err)
					allSuccessful = false
					return
				}
				if actualBytesRead != 4096 {
					fmt.Printf("ActualBytesRead %d != 4096\n", actualBytesRead)
					allSuccessful = false
					return
				}
				// Verifying returned data
				for k := offset; k < offset+int64(actualBytesRead); k++ {
					if buffer[k-offset] != generateByteAtOffset(int64(k)) {
						fmt.Printf("Invalid byte at offset %d\n", k)
						allSuccessful = false
						return
					}
				}
				offset += int64(actualBytesRead)
			}
		}()
	}
	join.Wait()
	fmt.Printf("Goroutines joined\n")
	reader.Close()
	// Verify statistics:
	assert.Equal(t, uint64(10*1000), hdfsAccessor.ReaderStats.ReadCount)
	// Probability of hitting Seek is very low, verifying that counter is withing reasonable range
	if hdfsAccessor.ReaderStats.SeekCount > 10 {
		t.Error("Too many seeks (more than 1%):", hdfsAccessor.ReaderStats.SeekCount)
	}
	assert.True(t, allSuccessful)
}

type MockRandomAccessHdfsAccessor struct {
	ReaderStats ReaderStats
}

var _ HdfsAccessor = (*MockRandomAccessHdfsAccessor)(nil) // ensure MockRandomAccessHdfsAccessor implements HdfsAccessor

func (this *MockRandomAccessHdfsAccessor) EnsureConnected() error {
	return errors.New("EnsureConnected is not implemented")
}

// Opens HDFS file for reading
func (this *MockRandomAccessHdfsAccessor) OpenRead(path string) (HdfsReader, error) {
	return &MockPseudoRandomHdfsReader{FileSize: int64(5 * 1024 * 1024 * 1024), ReaderStats: &this.ReaderStats}, nil
}

// Opens HDFS file for random access
func (this *MockRandomAccessHdfsAccessor) OpenReadForRandomAccess(path string) (RandomAccessHdfsReader, uint64, error) {
	return nil, 0, errors.New("OpenReadForRandomAccess is not implemented")

}

// Opens HDFS file for writing
func (this *MockRandomAccessHdfsAccessor) OpenWrite(path string) (HdfsWriter, error) {
	return nil, errors.New("OpenWrite is not implemented")
}

// Enumerates HDFS directory
func (this *MockRandomAccessHdfsAccessor) ReadDir(path string) ([]Attrs, error) {
	return nil, errors.New("ReadDir is not implemented")
}

// retrieves file/directory attributes
func (this *MockRandomAccessHdfsAccessor) Stat(path string) (Attrs, error) {
	return Attrs{Name: "5GB.blob", Size: 5 * 1024 * 1024 * 1024}, nil
}

// Creates a directory
func (this *MockRandomAccessHdfsAccessor) Mkdir(path string, mode os.FileMode) error {
	return errors.New("MkDir is not implemented")
}
