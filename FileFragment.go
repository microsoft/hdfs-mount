// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
)

// Represents a buffered (or cached) sequential fragment of the file.
// This could be data read from the file, or a write buffer
type FileFragment struct {
	Offset int64  // offset from the beginning of the file
	Data   []byte // data
}

// Reads into the file fragment buffer from the backend
func (this *FileFragment) ReadFromBackend(hdfsReader HdfsReader, offset *int64, minBytesToRead int, maxBytesToRead int) error {
	this.Data = make([]byte, maxBytesToRead)
	totalRead := 0
	this.Offset = *offset
	var err error = nil
	var nr int
	for totalRead < minBytesToRead {
		nr, err = hdfsReader.Read(this.Data[totalRead:maxBytesToRead])
		if err != nil {
			break
		}
		*offset += int64(nr)
		totalRead += nr
	}
	this.Data = this.Data[0:totalRead]
	return err
}

// Attempts to satisfy a read request using buffered data, returns true if successful
func (this *FileFragment) ReadFromBuffer(req *fuse.ReadRequest, resp *fuse.ReadResponse) bool {
	// computing a [start,end) range within this frament
	start := req.Offset - this.Offset
	if start < 0 || start >= int64(len(this.Data)) {
		return false
	}

	end := start + int64(req.Size)
	if end > int64(len(this.Data)) {
		end = int64(len(this.Data))
	}
	resp.Data = this.Data[start:end]
	return true
}
