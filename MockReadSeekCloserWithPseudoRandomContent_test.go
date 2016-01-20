// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"io"
	"math/rand"
	"time"
)

// This mock reader produces virtual 5G file with programmatically-generated pseudo-random content
// where each byte is a determianistic function of its offset, so it is easy to verify
// whether readinf of a chunk returns correct byte sequence
type MockReadSeekCloserWithPseudoRandomContent struct {
	Rand        *rand.Rand
	FileSize    int64
	position    int64
	IsClosed    bool
	ReaderStats *ReaderStats
}

// Seek to a given position
func (this *MockReadSeekCloserWithPseudoRandomContent) Seek(pos int64) error {
	this.position = pos
	this.ReaderStats.IncrementSeek()
	return nil
}

// Returns current posistion
func (this *MockReadSeekCloserWithPseudoRandomContent) Position() (int64, error) {
	return this.position, nil
}

// Reads chunk into the specified buffer
func (this *MockReadSeekCloserWithPseudoRandomContent) Read(buf []byte) (int, error) {
	// Sleeping for 1ms to yield to other threads
	time.Sleep(1 * time.Millisecond)
	this.ReaderStats.IncrementRead()
	if this.position >= this.FileSize {
		return 0, io.EOF
	}
	if len(buf) == 0 {
		return 0, nil
	}
	// Deciding how many bytes to return
	var nr int
	if this.Rand == nil {
		// If randomized isn't provided then returning as many as requested
		nr = len(buf)
	} else {
		// Otherwise random length:
		nr = this.Rand.Intn(len(buf)) + 1
	}

	// Adjusting for the case of the reading close to the end of the file
	if int64(nr) > this.FileSize-this.position {
		nr = int(this.FileSize - this.position)
	}
	// Programmatically generating data
	for i := 0; i < nr; i++ {
		buf[i] = generateByteAtOffset(this.position + int64(i))
	}
	this.position += int64(nr)
	return nr, nil
}

// Closes all underlying network connections
func (this *MockReadSeekCloserWithPseudoRandomContent) Close() error {
	this.IsClosed = true
	return nil
}

// Getting last 8 bits of a sum of remainders of a division to various prime numbers
// this gives us pseudo-random file content which is good enough for testing scenarios
func generateByteAtOffset(o int64) byte {
	return byte(o%7 + o%11 + o%13 + o%127 + o%251 + o%31337 + o%1299709)
}
