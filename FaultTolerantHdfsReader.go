// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

// Implements HdfsReader interface with automatic retries (acts as a proxy to HdfsReader)
type FaultTolerantHdfsReader struct {
	Impl HdfsReader
}

var _ HdfsReader = (*FaultTolerantHdfsReader)(nil) // ensure FaultTolerantHdfsReaderImpl implements HdfsReader
// Creates new instance of FaultTolerantHdfsReader
func NewFaultTolerantHdfsReader(impl HdfsReader) *FaultTolerantHdfsReader {
	return &FaultTolerantHdfsReader{Impl: impl}
}

// Read a chunk of data
func (this *FaultTolerantHdfsReader) Read(buffer []byte) (int, error) {
	return this.Impl.Read(buffer)
}

// Seeks to a given position
func (this *FaultTolerantHdfsReader) Seek(pos int64) error {
	return this.Impl.Seek(pos)
}

// Closes the stream
func (this *FaultTolerantHdfsReader) Close() error {
	return this.Impl.Close()
}
