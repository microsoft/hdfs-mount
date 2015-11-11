// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"errors"
	"golang.org/x/net/context"
	"io"
	"log"
)

// Encapsulates state and routines for reading data from the file handle
// FileHandleReader implements simple two-buffer scheme which allows to efficiently
// handle unordered reads which aren't far away from each other, so backend stream can
// be read sequentially without seek
type FileHandleReader struct {
	HdfsReader HdfsReader    // Backend reader
	Offset     int64         // Current offset for backend reader
	Buffer1    *FileFragment // Most recent fragment from the backend reader
	Buffer2    *FileFragment // Least recent fragment read from the backend
	Holes      int64         // tracks number of encountered "holes" TODO: find better name
	CacheHits  int64         // tracks number of cache hits (read requests from buffer)
	Seeks      int64         // tracks number of seeks performed on the backend stream
}

// Opens the reader (creates backend reader)
func (this *FileHandleReader) Open(handle *FileHandle) error {
	var err error
	this.HdfsReader, err = handle.File.FileSystem.HdfsAccessor.OpenRead(handle.File.AbsolutePath())
	if err != nil {
		log.Printf("ERROR opening %s: %s", handle.File.Attrs.Name, err)
		return err
	}
	this.Buffer1 = &FileFragment{}
	this.Buffer2 = &FileFragment{}
	return nil
}

var BLOCKSIZE int = 65536

// Responds on FUSE Read request
func (this *FileHandleReader) Read(handle *FileHandle, ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	//log.Printf("[%d] Read: offset=%d, size=%d", this.Offset, req.Offset, req.Size)
	// First checking whether we can satisfy request from buffered file fragments
	if this.Buffer1.ReadFromBuffer(req, resp) || this.Buffer2.ReadFromBuffer(req, resp) {
		this.CacheHits++
		return nil
	}

	// None of the buffers has the data to satisfy the request, we're going to read more data from backend into Buffer1

	// Before doing that, swapping buffers to keep MRU/LRU invariant
	this.Buffer2, this.Buffer1 = this.Buffer1, this.Buffer2

	maxBytesToRead := req.Size
	minBytesToRead := 1

	if req.Offset != this.Offset {
		// We're reading not from the offset expected by the backend stream
		// we need to decide whether we do Seek(), or read the skipped data (refered as "hole" below)
		if req.Offset > this.Offset && req.Offset-this.Offset <= int64(BLOCKSIZE*2) {
			holeSize := int(req.Offset - this.Offset)
			this.Holes++
			maxBytesToRead += holeSize    // we're going to read the "hole"
			minBytesToRead = holeSize + 1 // we need to read at least one byte starting from requested offset
		} else {
			this.Seeks++
			err := this.HdfsReader.Seek(req.Offset)
			if err != nil {
				log.Printf("[%d] Seek error to %d: %s", req.Offset, err.Error())
				return err
			}
			this.Offset = req.Offset
		}
	}

	// Ceiing to the nearest BLOCKSIZE
	maxBytesToRead = (maxBytesToRead + BLOCKSIZE - 1) / BLOCKSIZE * BLOCKSIZE

	// Reading from backend into Buffer1
	err := this.Buffer1.ReadFromBackend(this.HdfsReader, &this.Offset, minBytesToRead, maxBytesToRead)
	if err != nil {
		if err == io.EOF {
			log.Printf("[%s] EOF @%d", handle.File.AbsolutePath(), this.Offset)
			resp.Data = []byte{}
			return nil
		}
		return err
	}
	// Now Buffer1 has the data to satisfy request
	if !this.Buffer1.ReadFromBuffer(req, resp) {
		return errors.New("INTERNAL ERROR: FileFragment invariant")
	}
	return nil
}

// Closes the reader
func (this *FileHandleReader) Close() error {
	log.Printf("Handle stats: holes: %d, cache hits: %d, hard seeks: %d", this.Holes, this.CacheHits, this.Seeks)
	this.HdfsReader.Close()
	return nil
}
