// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"log"
	"sync"
	"time"
)

// Represends a handle to an open file
type FileHandle struct {
	File   *File
	Reader *FileHandleReader
	Writer *FileHandleWriter
	Mutex  sync.Mutex // all operations on the handle are serialized to simplify invariants
}

// Verify that *FileHandle implements necesary FUSE interfaces
var _ fs.Node = (*FileHandle)(nil)
var _ fs.HandleReader = (*FileHandle)(nil)
var _ fs.HandleReleaser = (*FileHandle)(nil)
var _ fs.HandleWriter = (*FileHandle)(nil)

// Creates new file handle
func NewFileHandle(file *File) *FileHandle {
	return &FileHandle{File: file}
}

// Opens handle for read mode
func (this *FileHandle) EnableRead() error {
	if this.Reader != nil {
		return nil
	}
	reader, err := NewFileHandleReader(this)
	if err != nil {
		return err
	}
	this.Reader = reader
	return nil
}

// Opens handle for write mode
func (this *FileHandle) EnableWrite(newFile bool) error {
	if this.Writer != nil {
		return nil
	}
	writer, err := NewFileHandleWriter(this, newFile)
	if err != nil {
		return err
	}
	this.Writer = writer
	return nil
}

// Returns attributes of the file associated with this handle
func (this *FileHandle) Attr(ctx context.Context, a *fuse.Attr) error {
	return this.File.Attr(ctx, a)
}

// Responds to FUSE Read request
func (this *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	this.Mutex.Lock()
	defer this.Mutex.Unlock()

	if this.Reader == nil {
		log.Printf("[%s] Warning: reading file opened for write @%d", this.File.AbsolutePath(), req.Offset)
		err := this.EnableRead()
		if err != nil {
			return err
		}
	}

	return this.Reader.Read(this, ctx, req, resp)
}

// Responds to FUSE Write request
func (this *FileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	this.Mutex.Lock()
	defer this.Mutex.Unlock()
	if this.Writer == nil {
		err := this.EnableWrite(false)
		if err != nil {
			return err
		}
	}
	return this.Writer.Write(this, ctx, req, resp)
}

// Responds to the FUSE Flush request
func (this *FileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	this.Mutex.Lock()
	defer this.Mutex.Unlock()
	if this.Writer != nil {
		return this.Writer.Flush()
	}
	return nil
}

// Closes the handle
func (this *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	if this.Reader != nil {
		err := this.Reader.Close()
		log.Printf("[%s] read-close: %s", this.File.AbsolutePath(), err)
		this.Reader = nil
	}
	if this.Writer != nil {
		err := this.Writer.Close()
		log.Printf("[%s] write-close: %s", this.File.AbsolutePath(), err)
		this.Writer = nil
	}
	// Invalidating metadata cache
	this.File.Attrs.Expires = this.File.FileSystem.Clock.Now().Add(-1 * time.Second)
	return nil
}
