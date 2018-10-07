// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

// Testing retry logic for EnsureConnected()
func TestEnsureConnectedWithRetries(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, atMost2Attempts())
	hdfsAccessor.EXPECT().EnsureConnected().Return(errors.New("Injected failure"))
	hdfsAccessor.EXPECT().EnsureConnected().Return(nil)
	err := ftHdfsAccessor.EnsureConnected()
	assert.Nil(t, err)
}

// Testing retry logic for Stat()
func TestStatWithRetries(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, atMost2Attempts())
	hdfsAccessor.EXPECT().Stat("/test/file").Return(Attrs{}, errors.New("Injected failure"))
	hdfsAccessor.EXPECT().Stat("/test/file").Return(Attrs{Name: "file"}, nil)
	hdfsAccessor.EXPECT().Close().Return(nil)
	attrs, err := ftHdfsAccessor.Stat("/test/file")
	assert.Nil(t, err)
	assert.Equal(t, "file", attrs.Name)
}

// Testing retry logic for Mkdir()
func TestMkdirWithRetries(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, atMost2Attempts())
	hdfsAccessor.EXPECT().Mkdir("/test/dir", os.FileMode(0757)).Return(errors.New("Injected failure"))
	hdfsAccessor.EXPECT().Mkdir("/test/dir", os.FileMode(0757)).Return(nil)
	hdfsAccessor.EXPECT().Close().Return(nil)
	err := ftHdfsAccessor.Mkdir("/test/dir", os.FileMode(0757))
	assert.Nil(t, err)
}

// Testing retry logic for ReadDir()
func TestReadDirWithRetries(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, atMost2Attempts())
	var result []Attrs
	var err error
	hdfsAccessor.EXPECT().ReadDir("/test/dir").Return(nil, errors.New("Injected failure"))
	hdfsAccessor.EXPECT().ReadDir("/test/dir").Return(make([]Attrs, 10), nil)
	hdfsAccessor.EXPECT().Close().Return(nil)
	result, err = ftHdfsAccessor.ReadDir("/test/dir")
	assert.Nil(t, err)
	assert.Equal(t, 10, len(result))
}

// Testing retry logic for OpenRead()
func TestOpenReadWithRetries(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, atMost2Attempts())
	mockReader := NewMockReadSeekCloser(mockCtrl)
	var result ReadSeekCloser
	var err error
	hdfsAccessor.EXPECT().OpenRead("/test/file").Return(nil, errors.New("Injected failure"))
	hdfsAccessor.EXPECT().OpenRead("/test/file").Return(mockReader, nil)
	hdfsAccessor.EXPECT().Close().Return(nil)
	result, err = ftHdfsAccessor.OpenRead("/test/file")
	assert.Nil(t, err)
	assert.Equal(t, mockReader, result.(*FaultTolerantHdfsReader).Impl)
}

// generates a test retry policy which allows 2 attempst
func atMost2Attempts() *RetryPolicy {
	clock := &MockClock{}
	rp := NewDefaultRetryPolicy(clock)
	rp.MaxAttempts = 2
	rp.TimeLimit = time.Hour
	return rp
}
