// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

// Implements ReadSeekCloser interface with automatic retries (acts as a proxy to HdfsReader)
type FaultTolerantHdfsReader struct {
	Path         string
	Impl         ReadSeekCloser
	HdfsAccessor HdfsAccessor
	RetryPolicy  *RetryPolicy
	Offset       int64
}

var _ ReadSeekCloser = (*FaultTolerantHdfsReader)(nil) // ensure FaultTolerantHdfsReaderImpl implements ReadSeekCloser
// Creates new instance of FaultTolerantHdfsReader
func NewFaultTolerantHdfsReader(path string, impl ReadSeekCloser, hdfsAccessor HdfsAccessor, retryPolicy *RetryPolicy) *FaultTolerantHdfsReader {
	return &FaultTolerantHdfsReader{Path: path, Impl: impl, HdfsAccessor: hdfsAccessor, RetryPolicy: retryPolicy}
}

// Read a chunk of data
func (this *FaultTolerantHdfsReader) Read(buffer []byte) (int, error) {
	op := this.RetryPolicy.StartOperation()
	for {
		var err error
		if this.Impl == nil {
			// Re-opening the file for read
			this.Impl, err = this.HdfsAccessor.OpenRead(this.Path)
			if err != nil {
				if op.ShouldRetry("[%s] OpenRead: %s", this.Path, err.Error()) {
					continue
				} else {
					return 0, err
				}
			}
			// Seeking to the right offset
			if err = this.Impl.Seek(this.Offset); err != nil {
				// Those errors are non-recoverable propagating right away
				this.Impl.Close()
				this.Impl = nil
				return 0, err
			}
		}
		// Performing the read
		var nr int
		nr, err = this.Impl.Read(buffer)
		if IsSuccessOrBenignError(err) || !op.ShouldRetry("[%s] Read @%d: %s", this.Path, this.Offset, err.Error()) {
			if err == nil {
				// On successful read, adjusting offset to the actual number of bytes read
				this.Offset += int64(nr)
			}
			return nr, err
		}
		// On failure, we need to close the reader
		this.Impl.Close()
		// and reset it to nil, so next time we attempt to re-open the file
		this.Impl = nil
	}
}

// Seeks to a given position
func (this *FaultTolerantHdfsReader) Seek(pos int64) error {
	// Seek is implemented as virtual operation on which doesn't involve communication,
	// passing that through without retires and promptly propagate errors
	// (which will be non-recoverable in this case)
	err := this.Impl.Seek(pos)
	if err == nil {
		// On success, updating current readng position
		this.Offset = pos
	}
	return err
}

// Returns current position
func (this *FaultTolerantHdfsReader) Position() (int64, error) {
	// This fault-tolerant wrapper keeps track the position on its own, no need
	// to query the backend
	return this.Offset, nil
}

// Closes the stream
func (this *FaultTolerantHdfsReader) Close() error {
	return this.Impl.Close()
}
