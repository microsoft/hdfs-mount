// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

// Testing retry logic for Read()
func TestSeekAndReadWithRetries(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsReader := NewMockHdfsReader(mockCtrl)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	ftHdfsReader := NewFaultTolerantHdfsReader("/path/to/file", hdfsReader, hdfsAccessor, atMost2Attempts())

	var err error
	var nr int
	// Performing succesfull read of 60 bytes of requested 100 at offset 1000
	hdfsReader.EXPECT().Seek(int64(1000)).Return(nil)
	err = ftHdfsReader.Seek(1000)
	assert.Nil(t, err)
	hdfsReader.EXPECT().Read(gomock.Any()).Return(60, nil)
	nr, err = ftHdfsReader.Read(make([]byte, 100))
	assert.Nil(t, err)
	assert.Equal(t, 60, nr)
	// Now the stream should be at position 160

	// Requesting one more read of 200 bytes, but this time it will fail
	hdfsReader.EXPECT().Read(gomock.Any()).Return(0, errors.New("Injected failure"))
	// As a result, ftHdfsReader should close the stream...
	hdfsReader.EXPECT().Close().Return(nil)
	// ...and invoke an OpenRead() to get new HdfsReader
	newHdfsReader := NewMockHdfsReader(mockCtrl)
	hdfsAccessor.EXPECT().OpenRead("/path/to/file").Return(newHdfsReader, nil)
	// It should seek at corret position (1060), and repeat the read
	newHdfsReader.EXPECT().Seek(int64(1060)).Return(nil)
	newHdfsReader.EXPECT().Read(gomock.Any()).Return(150, nil)
	nr, err = ftHdfsReader.Read(make([]byte, 200))
	assert.Nil(t, err)
	assert.Equal(t, 150, nr)
}

// No retries on benigh errors (e.g. EOF)
func TestNoRetryOnEOF(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsReader := NewMockHdfsReader(mockCtrl)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	ftHdfsReader := NewFaultTolerantHdfsReader("/path/to/file", hdfsReader, hdfsAccessor, atMost2Attempts())

	var err error
	var nr int
	hdfsReader.EXPECT().Read(gomock.Any()).Return(0, io.EOF)
	nr, err = ftHdfsReader.Read(make([]byte, 100))
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 0, nr)
}
