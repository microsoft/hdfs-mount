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
	InitLogger(os.Stdout, os.Stdout, os.Stdout, os.Stderr)
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

// Testing Chmod and Chown
func TestSetattr(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"foo", "bar"}, false, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().Mkdir("/foo", os.FileMode(0757)|os.ModeDir).Return(nil)
	node, _ := root.(*Dir).Mkdir(nil, &fuse.MkdirRequest{Name: "foo", Mode: os.FileMode(0757) | os.ModeDir})
	hdfsAccessor.EXPECT().Chmod("/foo", os.FileMode(0777)).Return(nil)
	err := node.(*Dir).Setattr(nil, &fuse.SetattrRequest{Mode: os.FileMode(0777), Valid: fuse.SetattrMode}, &fuse.SetattrResponse{})
	assert.Nil(t, err)
	assert.Equal(t, os.FileMode(0777), node.(*Dir).Attrs.Mode)

	hdfsAccessor.EXPECT().Chown("/foo", "root", "root").Return(nil)
	err = node.(*Dir).Setattr(nil, &fuse.SetattrRequest{Uid: 0, Valid: fuse.SetattrUid}, &fuse.SetattrResponse{})
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), node.(*Dir).Attrs.Uid)
}

// Testing Remove
func TestRemove(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"foo", "bar"}, false, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().Mkdir("/foo", os.FileMode(0757)|os.ModeDir).Return(nil)
	_, _ = root.(*Dir).Mkdir(nil, &fuse.MkdirRequest{Name: "foo", Mode: os.FileMode(0757) | os.ModeDir})

	hdfsAccessor.EXPECT().Remove("/foo").Return(nil)
	err := root.(*Dir).Remove(nil, &fuse.RemoveRequest{Name: "foo"})
	assert.Nil(t, err)

	// Lookup if the file exists
	hdfsAccessor.EXPECT().Stat("/foo").Return(Attrs{}, &os.PathError{"stat", "foo", os.ErrNotExist})
	_, err = root.(*Dir).Lookup(nil, "foo")
	assert.Equal(t, fuse.ENOENT, err) // Not found error, since it is deleted
}

// Testing Rename
func TestRename(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"foo", "bar"}, false, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().Mkdir("/foo", os.FileMode(0757)|os.ModeDir).Return(nil)
	node, _ := root.(*Dir).Mkdir(nil, &fuse.MkdirRequest{Name: "foo", Mode: os.FileMode(0757) | os.ModeDir})

	hdfsAccessor.EXPECT().Mkdir("/foo/test-rename", os.FileMode(0757)|os.ModeDir).Return(nil)
	_, _ = node.(*Dir).Mkdir(nil, &fuse.MkdirRequest{Name: "test-rename", Mode: os.FileMode(0757) | os.ModeDir})

	hdfsAccessor.EXPECT().Rename("/foo/test-rename", "/foo/foo-new").Return(nil)
	err := node.(*Dir).Rename(nil, &fuse.RenameRequest{OldName: "test-rename", NewName: "foo-new"}, node)
	assert.Nil(t, err)

	// Lookup if the file exists
	hdfsAccessor.EXPECT().Stat("/foo/test-rename").Return(Attrs{}, &os.PathError{"stat", "test-rename", os.ErrNotExist})
	_, err = node.(*Dir).Lookup(nil, "test-rename")
	assert.Equal(t, fuse.ENOENT, err) // Not found error, since it is renamed
	hdfsAccessor.EXPECT().Stat("/foo/foo-new").Return(Attrs{Name: "foo-new"}, nil)
	stat, err := node.(*Dir).Lookup(nil, "foo-new")
	assert.Nil(t, err)
	assert.Equal(t, "foo-new", stat.(*Dir).Attrs.Name)
}

// Testing Remove .Trash files
func TestRemoveTrash(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"foo", "bar"}, false, NewDefaultRetryPolicy(mockClock), mockClock)
	root, _ := fs.Root()
	hdfsAccessor.EXPECT().Mkdir("/foo", os.FileMode(0757)|os.ModeDir).Return(nil)
	node, _ := root.(*Dir).Mkdir(nil, &fuse.MkdirRequest{Name: "foo", Mode: os.FileMode(0757) | os.ModeDir})

	// Trying to remove .Trash directory on HDFS
	hdfsAccessor.EXPECT().Mkdir("/foo/.Trash", os.FileMode(0757) | os.ModeDir).Return(nil)
	_, _ = node.(*Dir).Mkdir(nil, &fuse.MkdirRequest{Name: ".Trash", Mode: os.FileMode(0757) | os.ModeDir})

	hdfsAccessor.EXPECT().Remove("/foo/.Trash").Return(nil)
	err := node.(*Dir).Remove(nil, &fuse.RemoveRequest{Name: ".Trash"})
	assert.Nil(t, err)

	hdfsAccessor.EXPECT().Stat("/foo/.Trash").Return(Attrs{Name: ".Trash"}, nil)
	stat, err := node.(*Dir).Lookup(nil, ".Trash")
	assert.Nil(t, err)
	assert.Equal(t, ".Trash", stat.(*Dir).Attrs.Name) // Found, as .Trash cannot be deleted
}
