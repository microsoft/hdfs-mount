package main

import (
	"sync/atomic"
)

type ReaderStats struct {
	ReadCount uint64
	SeekCount uint64
}

func (this *ReaderStats) IncrementRead() {
	if this != nil {
		atomic.AddUint64(&this.ReadCount, 1)
	}
}

func (this *ReaderStats) IncrementSeek() {
	if this != nil {
		atomic.AddUint64(&this.SeekCount, 1)
	}
}
