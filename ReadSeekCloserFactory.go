// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

// Interface to open a file for reading (create instance of ReadSeekCloser)
type ReadSeekCloserFactory interface {
	OpenRead() (ReadSeekCloser, error) // Opens a file to read with ReadSeekCloser interface
}
