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

// Returns current time
func (WallClock) Now() time.Time { return time.Now() }

func (WallClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}
