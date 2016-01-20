// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
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
	fs, _ := NewFileSystem(hdfsAccessor, "/tmp/x", []string{"*"}, true, mockClock)
	st, err := os.Stat(testZipPath())
	zipFile, err := os.Open(testZipPath())
	hdfsAccessor.EXPECT().OpenReadForRandomAccess("/foo/test.zip").Return(RandomAccessReader(zipFile), uint64(st.Size()), err)
	zipRootDir := NewZipRootDir(fs, "/foo/test.zip", Attrs{Name: "test.zip@", Mode: 0777 | os.ModeDir})
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
}
