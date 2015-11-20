// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"io"
	"math/rand"
	"sync"
	"time"
)

// Encapsulates a file handle for a file inside a zip archive
type ZipFileHandle struct {
	ContentStream io.ReadCloser
	lock          sync.Mutex
	offset        int64
}

// Ensure ZipFileHandle implements necesary fuse interface
var _ fs.Handle = (*ZipFileHandle)(nil)
var _ fs.HandleReleaser = (*ZipFileHandle)(nil)
var _ fs.HandleReader = (*ZipFileHandle)(nil)

// Creates new file handle
func NewZipFileHandle(contentStream io.ReadCloser) *ZipFileHandle {
	return &ZipFileHandle{ContentStream: contentStream}
}

// Releases (closes) the handle
func (this *ZipFileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return this.ContentStream.Close()
}

// Responds on FUSE Read request
func (this *ZipFileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	this.lock.Lock()
	defer this.lock.Unlock()
	for req.Offset != this.offset {
		// Since file is opened in fuse.OpenNonSeekable mode, we expect kernel to issue sequential reads.
		// However kernel might issue multiple read-ahead requests, one after another, but and they might be
		// reordered by underlying bazil/fuse library because it fans out each request to a separate concurrent goroutine.
		// If we got offset which isn't expected, this means that "wrong" goroutine grabbed the lock,
		// in this case yielding for other instance of concurrent go-routine.
		// This is a temporary workaround, we'll need to find better solution
		// TODO: consider addressing this at bazil/fuse, by adding per-handle request serialization feature which preserves ordering
		this.lock.Unlock()
		time.Sleep(time.Duration(rand.Int31n(10)) * time.Millisecond)
		this.lock.Lock()
	}

	// reading requested bytes
	buffer := make([]byte, req.Size)
	nr, err := io.ReadFull(this.ContentStream, buffer)
	this.offset += int64(nr)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		// EOF isn't an error from the FUSE's point of view
		err = nil
	}
	resp.Data = buffer[:nr]
	return err
}
