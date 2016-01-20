// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import ()

// Implements simple Read()/Seek()/Close() interface to read from a file or stream
// Concurrency: not thread safe: at most on request at a time
type ReadSeekCloser interface {
	Seek(pos int64) error            // Seeks to a given position
	Position() (int64, error)        // Returns current position
	Read(buffer []byte) (int, error) // Read a chunk of data
	Close() error                    // Closes the stream
}
