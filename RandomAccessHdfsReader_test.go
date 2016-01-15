// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"github.com/stretchr/testify/assert"
	//"math/rand"
	"testing"
)

// Basic test for HdfsRandomAccessReader
func TestHdfsRandomAccessReader(t *testing.T) {
	// Setting up mockery, to serve that large virtual file
	hdfsAccessor := &MockRandomAccessHdfsAccessor()
	reader, err := NewRandomAccessHdfsReader(hdfsAccessor, "/path/to/5G.blob")
	assert.Nil(t, err)
	// It should be able to report file size at this time:
	assert.Equal(t, fileSize, reader.Size)

	// Launching 10 parallel goroutines to concurrently read fragments of a file
	var join sync.WaitGroup
	for i := 0; i < 10; i++ {
		offset := int64(i) * 1024 * 1024 * 1024 / 3
		go func() {
			join.Add(1)
			defer join.Done()
			// Performing 1000 sequential reads in each gorutine
			for j := 0; j < 1000; j++ {
				buffer := make([]byte, 4096)
				nr, err := reader.ReadAt(buffer, offset)
				offset += nr
				assert.Nil(t, err)
				assert.NotEqual(t, 0, nr)
				// TODO: verify data
			}
		}()
	}
	join.Wait()
	// TODO: verify statistic
}

type MockRandomAccessHdfsAccessor struct {
}

var _ HdfsAccessor = (*MockRandomAccessHdfsAccessor)(nil) // ensure MockRandomAccessHdfsAccessor implements HdfsAccessor

func (this *MockRandomAccessHdfsAccessor) EnsureConnected() error {
	return errors.New("EnsureConnected is not implemented")
}

// Opens HDFS file for reading
func (this *hdfsAccessorImpl) OpenRead(path string) (HdfsReader, error) {
	return &PseudoRandomHdfsReader{FileSize: int64(5 * 1024 * 1024 * 1024)}, nil
}

// Opens HDFS file for writing
func (this *hdfsAccessorImpl) OpenWrite(path string) (HdfsWriter, error) {
	return nil, errors.New("OpenWrite is not implemented")
}

// Enumerates HDFS directory
func (this *hdfsAccessorImpl) ReadDir(path string) ([]Attrs, error) {
	return nil, errors.New("ReadDir is not implemented")
}

// retrieves file/directory attributes
func (this *hdfsAccessorImpl) Stat(path string) (Attrs, error) {
	return &Attrs{Name: "5GB.blob", Size: 5 * 1024 * 1024 * 1024}
}
