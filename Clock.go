// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"time"
)

// Interface to get wall clock time
// (taking an indirection makes unit testing easier)
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type WallClock struct{}

var _ Clock = WallClock{} // ensure WallClock implements Clock

// Returns current time
func (WallClock) Now() time.Time {
	return time.Now()
}

// After waits for the duration to elapse and then sends the current time
// on the returned channel.
func (WallClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}
