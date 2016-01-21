// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
)

// Wraps FileHandle exposing it as ReadSeekCloser intrface
// Concurrency: not thread safe: at most on request at a time
type FileHandleAsReadSeekCloser struct {
	FileHandle *FileHandle
	Offset     int64
}

// Verify that *FileHandleAsReadSeekCloser implements ReadSeekCloser
var _ ReadSeekCloser = (*FileHandleAsReadSeekCloser)(nil)

// Creates new adapter
func NewFileHandleAsReadSeekCloser(fileHandle *FileHandle) ReadSeekCloser {
	return &FileHandleAsReadSeekCloser{FileHandle: fileHandle}
}

// Reads a chunk of data
func (this *FileHandleAsReadSeekCloser) Read(buffer []byte) (int, error) {
	resp := fuse.ReadResponse{Data: buffer}
	err := this.FileHandle.Read(nil, &fuse.ReadRequest{Offset: this.Offset, Size: len(buffer)}, &resp)
	this.Offset += int64(len(resp.Data))
	return len(resp.Data), err
}

// Seeks to a given position
func (this *FileHandleAsReadSeekCloser) Seek(pos int64) error {
	// Note: seek is implemented as virtual operation, error checking will happen
	// when a Read() is called after a problematic Seek()
	this.Offset = pos
	return nil
}

// Returns reading position
func (this *FileHandleAsReadSeekCloser) Position() (int64, error) {
	return this.Offset, nil
}

// Closes the underlying file handle
func (this *FileHandleAsReadSeekCloser) Close() error {
	return this.FileHandle.Release(nil, nil)
}
