// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/src/archive/zip"
	"golang.org/x/net/context"
)

// Encapsulates state and operations for a virtual file inside zip archive on HDFS file system
type ZipFile struct {
	Attrs      Attrs
	zipFile    *zip.File
	FileSystem *FileSystem
}

// Verify that *Dir implements necesary FUSE interfaces
var _ fs.Node = (*ZipFile)(nil)
var _ fs.NodeOpener = (*ZipFile)(nil)

// Responds on FUSE Attr request to retrieve file attributes
func (this *ZipFile) Attr(ctx context.Context, fuseAttr *fuse.Attr) error {
	return this.Attrs.Attr(fuseAttr)
}

// Responds on FUSE Open request for a file inside zip archive
func (this *ZipFile) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	this.FileSystem.OnFileOpened()
	contentStream, err := this.zipFile.Open()
	if err != nil {
		return nil, err
	}
	// reporting to FUSE that the stream isn't seekable
	resp.Flags |= fuse.OpenNonSeekable
	return NewZipFileHandle(contentStream), nil
}
