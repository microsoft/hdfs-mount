// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"runtime"
	"testing"
)

// Returns path to test.zip file
func testZipPath() string {
	// finding test file in the same directory as this unit test
	_, thisFile, _, _ := runtime.Caller(0)
	return path.Join(path.Dir(thisFile), "test.zip")
}

// Archive:  test.zip
//   Length      Date    Time    Name
// ---------  ---------- -----   ----
//         0  2015-11-26 16:54   foo/
//      1234  2015-11-26 16:53   foo/a
//         0  2015-11-26 16:54   foo/baz/
//         0  2015-11-26 16:54   foo/baz/x/
//         0  2015-11-26 16:54   foo/baz/x/y/
//         0  2015-11-26 16:54   foo/baz/x/y/z/
//       256  2015-11-26 16:54   foo/baz/x/y/z/w
//      4321  2015-11-26 16:53   foo/b
//         0  2015-11-26 16:53   foo/bar/
//       256  2015-11-26 16:53   foo/bar/c
//      1024  2015-11-26 16:53   qux
// ---------                     -------
//      7091                     11 files

// Testing ZipDir.ReadArchive functionality
func TestZipDirReadArchive(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockClock := &MockClock{}
	hdfsAccessor := NewMockHdfsAccessor(mockCtrl)
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"*"}, true, false, NewDefaultRetryPolicy(mockClock), mockClock)
	zipFile, err := os.Open(testZipPath())
	assert.Nil(t, err)
	zipFileInfo, err := zipFile.Stat()
	assert.Nil(t, err)
	hdfsAccessor.EXPECT().Stat("/test.zip").Return(Attrs{Name: "test.zip", Size: uint64(zipFileInfo.Size())}, nil)
	hdfsAccessor.EXPECT().OpenRead("/test.zip").Return(ReadSeekCloser(&FileAsReadSeekCloser{File: zipFile}), err)
	root, err := fs.Root()
	zipRootDirNode, err := root.(*Dir).Lookup(nil, "test.zip@")
	assert.Nil(t, err)
	zipRootDir := zipRootDirNode.(*ZipDir)

	foo, err := zipRootDir.Lookup(nil, "foo")
	assert.Nil(t, err)
	assert.Equal(t, "foo", foo.(*ZipDir).Attrs.Name)

	a, err := foo.(*ZipDir).Lookup(nil, "a")
	assert.Nil(t, err)
	assert.Equal(t, "a", a.(*ZipFile).Attrs.Name)
	assert.Equal(t, uint64(1234), a.(*ZipFile).Attrs.Size)

	baz, err := foo.(*ZipDir).Lookup(nil, "baz")
	assert.Nil(t, err)
	assert.Equal(t, "baz", baz.(*ZipDir).Attrs.Name)

	x, err := baz.(*ZipDir).Lookup(nil, "x")
	assert.Nil(t, err)
	assert.Equal(t, "x", x.(*ZipDir).Attrs.Name)

	y, err := x.(*ZipDir).Lookup(nil, "y")
	assert.Nil(t, err)
	assert.Equal(t, "y", y.(*ZipDir).Attrs.Name)

	z, err := y.(*ZipDir).Lookup(nil, "z")
	assert.Nil(t, err)
	assert.Equal(t, "z", z.(*ZipDir).Attrs.Name)

	w, err := z.(*ZipDir).Lookup(nil, "w")
	assert.Nil(t, err)
	assert.Equal(t, "w", w.(*ZipFile).Attrs.Name)
	assert.Equal(t, uint64(256), w.(*ZipFile).Attrs.Size)

	b, err := foo.(*ZipDir).Lookup(nil, "b")
	assert.Nil(t, err)
	assert.Equal(t, "b", b.(*ZipFile).Attrs.Name)
	assert.Equal(t, uint64(4321), b.(*ZipFile).Attrs.Size)

	bar, err := foo.(*ZipDir).Lookup(nil, "bar")
	assert.Nil(t, err)
	assert.Equal(t, "bar", bar.(*ZipDir).Attrs.Name)

	qux, err := zipRootDir.Lookup(nil, "qux")
	assert.Nil(t, err)
	assert.Equal(t, "qux", qux.(*ZipFile).Attrs.Name)
	assert.Equal(t, uint64(1024), qux.(*ZipFile).Attrs.Size)

	// Test ReadDirAll
	entries, err := zipRootDir.ReadDirAll(nil)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(entries)) // Number of subdirs and files

	// Test ZipFile Attr
	fuseAttr := &fuse.Attr{}
	err = b.Attr(nil, fuseAttr)
	assert.Nil(t, err)
	assert.Equal(t, uint64(4321), fuseAttr.Size)

	// Test ZipFile Open
	zipFileHandle, err := b.(*ZipFile).Open(nil, &fuse.OpenRequest{}, &fuse.OpenResponse{})
	assert.Nil(t, err)

	// Test ZipFile Read
	err = zipFileHandle.(*ZipFileHandle).Read(nil, &fuse.ReadRequest{Size: 10}, &fuse.ReadResponse{})
	assert.Nil(t, err)

	// Test ZipFile Release
	err = zipFileHandle.(*ZipFileHandle).Release(nil, &fuse.ReleaseRequest{})
	assert.Nil(t, err)
}

// ReadSeekCloser adapter for os.File
type FileAsReadSeekCloser struct {
	File *os.File
}

// Reads a chunk of data
func (this *FileAsReadSeekCloser) Read(buffer []byte) (int, error) {
	return this.File.Read(buffer)
}

// Seeks to a given position
func (this *FileAsReadSeekCloser) Seek(pos int64) error {
	_, err := this.File.Seek(pos, 0)
	return err
}

// Returns reading position
func (this *FileAsReadSeekCloser) Position() (int64, error) {
	return this.File.Seek(0, 1)
}

func (this *FileAsReadSeekCloser) Close() error {
	return this.File.Close()
}
