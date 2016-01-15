// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"errors"
	"github.com/colinmarc/hdfs"
)

// Allows to open an HDFS file as a seekable read-only stream
// Concurrency: not thread safe: at most on request at a time
type HdfsReader interface {
	Seek(pos int64) error            // Seeks to a given position
	Position() (int64, error)        // Returns current position
	Read(buffer []byte) (int, error) // Read a chunk of data
	Close() error                    // Closes the stream
}

type hdfsReaderImpl struct {
	BackendReader *hdfs.FileReader
}

var _ HdfsReader = (*hdfsReaderImpl)(nil) // ensure hdfsReaderImpl implements HdfsReader

// Creates new instance of HdfsReader
func NewHdfsReader(backendReader *hdfs.FileReader) HdfsReader {
	return &hdfsReaderImpl{BackendReader: backendReader}
}

// Read a chunk of data
func (this *hdfsReaderImpl) Read(buffer []byte) (int, error) {
	return this.BackendReader.Read(buffer)
}

// Seeks to a given position
func (this *hdfsReaderImpl) Seek(pos int64) error {
	actualPos, err := this.BackendReader.Seek(pos, 0)
	if err != nil {
		return err
	}
	if pos != actualPos {
		return errors.New("Can't seek to requested position")
	}
	return nil
}

// Returns current position
func (this *hdfsReaderImpl) Position() (int64, error) {
	actualPos, err := this.BackendReader.Seek(0, 1)
	if err != nil {
		return 0, err
	}
	return actualPos, nil
}

// Closes the stream
func (this *hdfsReaderImpl) Close() error {
	return this.BackendReader.Close()
}
