// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	"os"
	"testing"
)

func TestFileSetattr(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"foo", "bar"}, false, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()

	hdfswriter := NewMockHdfsWriter(mockCtrl)
	hdfsAccessor.EXPECT().Remove("/testFileSetattr").Return(nil)

	hdfsAccessor.EXPECT().CreateFile("/testFileSetattr", os.FileMode(0757)).Return(hdfswriter, nil)
	file := root.(*Dir).NodeFromAttrs(Attrs{Name: "/testFileSetattr", Mode: os.FileMode(0757), Uid: uint32(500), Gid: uint32(500)})

	hdfsAccessor.EXPECT().Chmod("/testFileSetattr", os.FileMode(0777)).Return(nil)
	err := file.(*File).Setattr(nil, &fuse.SetattrRequest{Mode: os.FileMode(0777), Valid: fuse.SetattrMode}, &fuse.SetattrResponse{})
	assert.Nil(t, err)
	assert.Equal(t, os.FileMode(0777), file.(*File).Attrs.Mode)
}
