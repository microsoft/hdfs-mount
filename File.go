// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"log"
	"path"
)

type File struct {
	FileSystem *FileSystem // pointer to the FieSystem which owns this file
	Attrs      Attrs       // Cache of file attributes // TODO: implement TTL
	Parent     *Dir        // Pointer to the parent directory (allows computing fully-qualified paths on demand)
}

// Verify that *File implements necesary FUSE interfaces
var _ fs.Node = (*File)(nil)
var _ fs.NodeOpener = (*File)(nil)

// Retunds absolute path of the file in HDFS namespace
func (this *File) AbsolutePath() string {
	return path.Join(this.Parent.AbsolutePath(), this.Attrs.Name)
}

// Responds to the FUSE file attribute request
func (this *File) Attr(ctx context.Context, a *fuse.Attr) error {
	if this.FileSystem.Clock.Now().After(this.Attrs.Expires) {
		err := this.Parent.LookupAttrs(this.Attrs.Name, &this.Attrs)
		if err != nil {
			return err
		}
	}
	return this.Attrs.Attr(a)
}

// Responds to the FUSE file open request (creates new file handle)
func (this *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	log.Printf("[%s] Opened", this.AbsolutePath())
	this.FileSystem.OnFileOpened()
	var err error
	handle, err := NewFileHandle(this)
	return handle, err
}
