// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"log"
	"sync"
)

// Represends a handle to an open file
type FileHandle struct {
	File   *File
	Reader FileHandleReader
	Mutex  sync.Mutex // all operations on the handle are serialized to simplify invariants
}

// Verify that *FileHandle implements necesary FUSE interfaces
var _ fs.Node = (*FileHandle)(nil)
var _ fs.HandleReader = (*FileHandle)(nil)
var _ fs.HandleReleaser = (*FileHandle)(nil)

// Creates new file handle
func NewFileHandle(file *File) (*FileHandle, error) {
	this := &FileHandle{File: file}
	return this, (&this.Reader).Open(this)
}

// Returns attributes of the file associated with this handle
func (this *FileHandle) Attr(ctx context.Context, a *fuse.Attr) error {
	return this.File.Attr(ctx, a)
}

// Reponds to FUSE Read request
func (this *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	this.Mutex.Lock()
	defer this.Mutex.Unlock()
	return this.Reader.Read(this, ctx, req, resp)
}

// Closes the handle
func (this *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	log.Printf("Handle closed")
	this.Reader.Close()
	return nil
}
