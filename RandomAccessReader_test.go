// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

// Basic test for HdfsRandomAccessReader
func TestHdfsRandomAccessReader(t *testing.T) {
	// Setting up mockery, to serve that large virtual file
	fileSize := int64(5 * 1024 * 1024 * 1024)
	file := &Mock5GFile{ReaderStats: &ReaderStats{}}
	reader := NewRandomAccessReader(file)
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
	assert.Equal(t, uint64(10*1000), file.ReaderStats.ReadCount)
	// Probability of hitting Seek is very low, verifying that counter is within reasonable range
	if file.ReaderStats.SeekCount > 10 {
		t.Error("Too many seeks (more than 1%):", file.ReaderStats.SeekCount)
	} else {
		fmt.Printf("Seek frequency: %g%%\n", float64(file.ReaderStats.SeekCount)/float64(file.ReaderStats.ReadCount)*100.0)
	}
	assert.True(t, allSuccessful)
}

type Mock5GFile struct {
	ReaderStats *ReaderStats
}

var _ ReadSeekCloserFactory = (*Mock5GFile)(nil) // ensure Mock5GFile  implements ReadSeekCloserFactory

func (this *Mock5GFile) OpenRead() (ReadSeekCloser, error) {
	return &MockReadSeekCloserWithPseudoRandomContent{FileSize: 5 * 1024 * 1024 * 1024, ReaderStats: this.ReaderStats}, nil
}
