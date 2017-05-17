// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIsPathAllowedForStarPrefix(t *testing.T) {
	fs, _ := NewFileSystem(nil, "/tmp", []string{"*"}, false, NewDefaultRetryPolicy(WallClock{}), WallClock{})
	assert.True(t, fs.IsPathAllowed("/"))
	assert.True(t, fs.IsPathAllowed("/foo"))
	assert.True(t, fs.IsPathAllowed("/foo/bar"))
}

func TestIsPathAllowedForMiscPrefixes(t *testing.T) {
	fs, _ := NewFileSystem(nil, "/tmp", []string{"foo", "bar", "baz/qux"}, false, NewDefaultRetryPolicy(WallClock{}), WallClock{})
	assert.True(t, fs.IsPathAllowed("/"))
	assert.True(t, fs.IsPathAllowed("/foo"))
	assert.True(t, fs.IsPathAllowed("/bar"))
	assert.True(t, fs.IsPathAllowed("/foo/x"))
	assert.True(t, fs.IsPathAllowed("/baz/qux"))
	assert.True(t, fs.IsPathAllowed("/baz/qux/y"))
	assert.False(t, fs.IsPathAllowed("qux"))
	assert.False(t, fs.IsPathAllowed("w/z"))
}

func TestStatfs(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"*"}, false, NewDefaultRetryPolicy(mockClock), mockClock)

	hdfsAccessor.EXPECT().StatFs().Return(FsInfo{capacity: uint64(10240), remaining: uint64(1024)}, nil)
	fsInfo := &fuse.StatfsResponse{}
	err := fs.Statfs(nil, &fuse.StatfsRequest{}, fsInfo)
	assert.Nil(t, err)
	assert.Equal(t, uint64(10), fsInfo.Blocks)
	assert.Equal(t, uint64(1), fsInfo.Bfree)
}
