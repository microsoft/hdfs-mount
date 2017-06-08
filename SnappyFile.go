// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"

	"github.com/golang/snappy"
)

// Encapsulates state and operations for a virtual file inside zip archive on HDFS file system
type SnappyFile struct {
	Attrs        Attrs
	snappyReader *snappy.Reader
	snappyWriter *snappy.Writer
	FileSystem   *FileSystem
}

// Verify that *Dir implements necesary FUSE interfaces
var _ fs.Node = (*SnappyFile)(nil)
var _ fs.NodeOpener = (*SnappyFile)(nil)

// Responds on FUSE Attr request to retrieve file attributes
func (this *SnappyFile) Attr(ctx context.Context, fuseAttr *fuse.Attr) error {
	return this.Attrs.Attr(fuseAttr)
}

// Responds on FUSE Open request for a file inside snappy archive
func (this *SnappyFile) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	
}
