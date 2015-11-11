// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

// Implements HdfsWriter interface with automatic retries (acts as a proxy to HdfsWriter)
type FaultTolerantHdfsWriter struct {
	Impl HdfsWriter
}

var _ HdfsWriter = (*FaultTolerantHdfsWriter)(nil) // ensure FaultTolerantHdfsWriterImpl implements HdfsWriter
// Creates new instance of FaultTolerantHdfsWriter
func NewFaultTolerantHdfsWriter(impl HdfsWriter) HdfsWriter {
	return &FaultTolerantHdfsWriter{Impl: impl}
}

// Seeks to a given position
func (this *FaultTolerantHdfsWriter) Seek(pos int64) error {
	// TODO: implement fault tolerance
	return this.Impl.Seek(pos)
}

// Writes chunk of data
func (this *FaultTolerantHdfsWriter) Write(buffer []byte) (int, error) {
	// TODO: implement fault tolerance
	return this.Impl.Write(buffer)
}

// Flushes all the data
func (this *FaultTolerantHdfsWriter) Flush() error {
	// TODO: implement fault tolerance
	return this.Impl.Flush()
}

// Closes the stream
func (this *FaultTolerantHdfsWriter) Truncate() error {
	// TODO: implement fault tolerance
	return this.Impl.Truncate()
}

// Truncate the HDFS file at a given position
func (this *FaultTolerantHdfsWriter) Close() error {
	// TODO: implement fault tolerance
	return this.Impl.Close()
}
