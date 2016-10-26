// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

// Testing whether attributes are cached
func TestAttributeCaching(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"*"}, false, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().Stat("/testDir").Return(Attrs{Name: "testDir", Mode: os.ModeDir | 0757}, nil)
	dir, err := root.(*Dir).Lookup(nil, "testDir")
	assert.Nil(t, err)
	// Second call to Lookup(), shouldn't re-issue Stat() on backend
	dir1, err1 := root.(*Dir).Lookup(nil, "testDir")
	assert.Nil(t, err1)
	assert.Equal(t, dir, dir1) // must return the same entry w/o doing Stat on the backend

	// Retrieving attributes from cache
	var attr fuse.Attr
	assert.Nil(t, dir.Attr(nil, &attr))
	assert.Equal(t, os.ModeDir|0757, attr.Mode)

	mockClock.NotifyTimeElapsed(30 * time.Second)
	assert.Nil(t, dir.Attr(nil, &attr))
	assert.Equal(t, os.ModeDir|0757, attr.Mode)

	// Lookup should be stil done from cache
	dir1, err1 = root.(*Dir).Lookup(nil, "testDir")
	assert.Nil(t, err1)

	// After 30+31=61 seconds, attempt to query attributes should re-issue a Stat() request to the backend
	// this time returing different attributes (555 instead of 757)
	hdfsAccessor.EXPECT().Stat("/testDir").Return(Attrs{Name: "testDir", Mode: os.ModeDir | 0555}, nil)
	mockClock.NotifyTimeElapsed(31 * time.Second)
	assert.Nil(t, dir.Attr(nil, &attr))
	assert.Equal(t, os.ModeDir|0555, attr.Mode)
	dir1, err1 = root.(*Dir).Lookup(nil, "testDir")
	assert.Nil(t, err1)
	assert.Equal(t, dir, dir1)
}

// Testing whether '-allowedPrefixes' path filtering works for ReadDir
func TestReadDirWithFiltering(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"foo", "bar"}, false, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().ReadDir("/").Return([]Attrs{
		Attrs{Name: "quz", Mode: os.ModeDir},
		Attrs{Name: "foo", Mode: os.ModeDir},
		Attrs{Name: "bar", Mode: os.ModeDir},
		Attrs{Name: "foobar", Mode: os.ModeDir},
		Attrs{Name: "baz", Mode: os.ModeDir},
	}, nil)
	dirents, err := root.(*Dir).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(dirents))
	assert.Equal(t, "foo", dirents[0].Name)
	assert.Equal(t, "bar", dirents[1].Name)
}

// Testing processing of .zip files if '-expandZips' isn't activated
func TestReadDirWithZipExpansionDisabled(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"*"}, false, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().ReadDir("/").Return([]Attrs{
		Attrs{Name: "foo.zipx"},
		Attrs{Name: "dir.zip", Mode: os.ModeDir},
		Attrs{Name: "bar.zip"},
	}, nil)
	dirents, err := root.(*Dir).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(dirents))
	assert.Equal(t, "foo.zipx", dirents[0].Name)
	assert.Equal(t, "dir.zip", dirents[1].Name)
	assert.Equal(t, "bar.zip", dirents[2].Name)
}

// Testing processing of .zip files if '-expandZips' is activated
func TestReadDirWithZipExpansionEnabled(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"*"}, true, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().ReadDir("/").Return([]Attrs{
		Attrs{Name: "foo.zipx"},
		Attrs{Name: "dir.zip", Mode: os.ModeDir},
		Attrs{Name: "bar.zip"},
	}, nil)
	dirents, err := root.(*Dir).ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(dirents))
	assert.Equal(t, "foo.zipx", dirents[0].Name)
	assert.Equal(t, "dir.zip", dirents[1].Name)
	assert.Equal(t, "bar.zip", dirents[2].Name)
	assert.Equal(t, fuse.DT_File, dirents[2].Type)
	assert.Equal(t, "bar.zip@", dirents[3].Name)
	assert.Equal(t, fuse.DT_Dir, dirents[3].Type)
}

// Testing whether '-allowedPrefixes' path filtering works for Lookup
func TestLookupWithFiltering(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"foo", "bar"}, false, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().Stat("/foo").Return(Attrs{Name: "foo", Mode: os.ModeDir}, nil)
	_, err := root.(*Dir).Lookup(nil, "foo")
	assert.Nil(t, err)
	_, err = root.(*Dir).Lookup(nil, "qux")
	assert.Equal(t, fuse.ENOENT, err) // Not found error, since it is not in the allowed prefixes
}

// Testing Mkdir
func TestMkdir(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"foo", "bar"}, false, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().Mkdir("/foo", os.FileMode(0757)|os.ModeDir).Return(nil)
	node, err := root.(*Dir).Mkdir(nil, &fuse.MkdirRequest{Name: "foo", Mode: os.FileMode(0757) | os.ModeDir})
	assert.Nil(t, err)
	assert.Equal(t, "foo", node.(*Dir).Attrs.Name)
}

// Tesing Chmod and Chown
func TestSetattr(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"foo", "bar"}, false, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().Mkdir("/foo", os.FileMode(0757)|os.ModeDir).Return(nil)
	node, _ := root.(*Dir).Mkdir(nil, &fuse.MkdirRequest{Name: "foo", Mode: os.FileMode(0757) | os.ModeDir})
	hdfsAccessor.EXPECT().Chmod("/foo", os.FileMode(0777)).Return(nil)
	err := node.(*Dir).Setattr(nil, &fuse.SetattrRequest{Mode: os.FileMode(0777)}, &fuse.SetattrResponse{})
	assert.Nil(t, err)
	assert.Equal(t, os.FileMode(0777), node.(*Dir).Attrs.Mode)
}
