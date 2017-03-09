// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"errors"
	"golang.org/x/net/context"
	"io"
)

// Encapsulates state and routines for reading data from the file handle
// FileHandleReader implements simple two-buffer scheme which allows to efficiently
// handle unordered reads which aren't far away from each other, so backend stream can
// be read sequentially without seek
type FileHandleReader struct {
	Handle     *FileHandle    // File handle
	HdfsReader ReadSeekCloser // Backend reader
	Offset     int64          // Current offset for backend reader
	Buffer1    *FileFragment  // Most recent fragment from the backend reader
	Buffer2    *FileFragment  // Least recent fragment read from the backend
	Holes      int64          // tracks number of encountered "holes" TODO: find better name
	CacheHits  int64          // tracks number of cache hits (read requests from buffer)
	Seeks      int64          // tracks number of seeks performed on the backend stream
}

// Opens the reader (creates backend reader)
func NewFileHandleReader(handle *FileHandle) (*FileHandleReader, error) {
	this := &FileHandleReader{Handle: handle}
	var err error
	this.HdfsReader, err = handle.File.FileSystem.HdfsAccessor.OpenRead(handle.File.AbsolutePath())
	if err != nil {
		Error.Println("[", handle.File.AbsolutePath(), "] Opening: ", err)
		return nil, err
	}
	this.Buffer1 = &FileFragment{}
	this.Buffer2 = &FileFragment{}
	return this, nil
}

// Responds on FUSE Read request. Note: If FUSE requested to read N bytes it expects exactly N, unless EOF
func (this *FileHandleReader) Read(handle *FileHandle, ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	totalRead := 0
	buf := resp.Data[0:req.Size]
	fileOffset := req.Offset
	var nr int
	var err error
	for len(buf) > 0 {
		nr, err = this.ReadPartial(handle, fileOffset, buf)
		if err != nil {
			break
		}
		totalRead += nr
		fileOffset += int64(nr)
		buf = buf[nr:]
	}
	resp.Data = resp.Data[0:totalRead]
	if err == io.EOF {
		// EOF isn't a error, reporting successful read to FUSE
		return nil
	} else {
		return err
	}
}

var BLOCKSIZE int = 65536

// Reads chunk of data (satisfies part of FUSE read request)
func (this *FileHandleReader) ReadPartial(handle *FileHandle, fileOffset int64, buf []byte) (int, error) {
	// First checking whether we can satisfy request from buffered file fragments
	var nr int
	if this.Buffer1.ReadFromBuffer(fileOffset, buf, &nr) || this.Buffer2.ReadFromBuffer(fileOffset, buf, &nr) {
		this.CacheHits++
		return nr, nil
	}

	// None of the buffers has the data to satisfy the request, we're going to read more data from backend into Buffer1

	// Before doing that, swapping buffers to keep MRU/LRU invariant
	this.Buffer2, this.Buffer1 = this.Buffer1, this.Buffer2

	maxBytesToRead := len(buf)
	minBytesToRead := 1

	if fileOffset != this.Offset {
		// We're reading not from the offset expected by the backend stream
		// we need to decide whether we do Seek(), or read the skipped data (refered as "hole" below)
		if fileOffset > this.Offset && fileOffset-this.Offset <= int64(BLOCKSIZE*2) {
			holeSize := int(fileOffset - this.Offset)
			this.Holes++
			maxBytesToRead += holeSize    // we're going to read the "hole"
			minBytesToRead = holeSize + 1 // we need to read at least one byte starting from requested offset
		} else {
			this.Seeks++
			err := this.HdfsReader.Seek(fileOffset)
			// If seek error happens, return err. Seek to the end of the file is not an error.
			if err != nil && this.Offset > fileOffset{
				Error.Println("[seek offset:", this.Offset, "] Seek error to", fileOffset, "(file offset):", err.Error())
				return 0, err
			}
			this.Offset = fileOffset
		}
	}

	// Ceiling to the nearest BLOCKSIZE
	maxBytesToRead = (maxBytesToRead + BLOCKSIZE - 1) / BLOCKSIZE * BLOCKSIZE

	// Reading from backend into Buffer1
	err := this.Buffer1.ReadFromBackend(this.HdfsReader, &this.Offset, minBytesToRead, maxBytesToRead)
	if err != nil {
		if err == io.EOF {
			Error.Println("[", handle.File.AbsolutePath(), "] EOF @", this.Offset)
			return 0, err
		}
		return 0, err
	}
	// Now Buffer1 has the data to satisfy request
	if !this.Buffer1.ReadFromBuffer(fileOffset, buf, &nr) {
		return 0, errors.New("INTERNAL ERROR: FileFragment invariant")
	}
	return nr, nil
}

// Closes the reader
func (this *FileHandleReader) Close() error {
	if this.HdfsReader != nil {
		Info.Println("[", this.Handle.File.AbsolutePath(), "] ReadStats: holes:", this.Holes, ", cache hits:", this.CacheHits, ", hard seeks:", this.Seeks)
		this.HdfsReader.Close()
		this.HdfsReader = nil
	}
	return nil
}
