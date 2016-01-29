// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"errors"
	"github.com/colinmarc/hdfs"
)

// Allows to open HDFS file as a seekable/flushable/truncatable write-only stream
// Concurrency: not thread safe: at most on request at a time
type HdfsWriter interface {
	Seek(pos int64) error             // Seeks to a given position
	Write(buffer []byte) (int, error) // Writes chunk of data
	Flush() error                     // Flushes all the data
	Close() error                     // Closes the stream
	Truncate() error                  // Truncate the HDFS file at a given position
}

type hdfsWriterImpl struct {
	BackendWriter *hdfs.FileWriter
}

var _ HdfsWriter = (*hdfsWriterImpl)(nil) // ensure hdfsWriterImpl implements HdfsWriter

// Creates new instance of HdfsWriter
func NewHdfsWriter(backendWriter *hdfs.FileWriter) HdfsWriter {
	return &hdfsWriterImpl{BackendWriter: backendWriter}
}

// Seeks to a given position
func (this *hdfsWriterImpl) Seek(pos int64) error {
	return errors.New("Seek is not implemented")
}

// Writes chunk of data
func (this *hdfsWriterImpl) Write(buffer []byte) (int, error) {
	return this.BackendWriter.Write(buffer)
}

// Flushes all the data
func (this *hdfsWriterImpl) Flush() error {
	return errors.New("Flush is not implemented")
}

// Closes the stream
func (this *hdfsWriterImpl) Truncate() error {
	return errors.New("Truncate is not implemented")
}

// Truncate the HDFS file at a given position
func (this *hdfsWriterImpl) Close() error {
	return this.BackendWriter.Close()
}
