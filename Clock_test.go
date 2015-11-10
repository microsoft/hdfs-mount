// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"time"
)

type MockClock struct {
	now time.Time
}

var _ Clock = (*MockClock)(nil) // ensure MockClock implements Clock

// Returns current time
func (this *MockClock) Now() time.Time {
	return this.now
}

// After waits for the duration to elapse and then sends the current time
// on the returned channel
func (this *MockClock) After(d time.Duration) <-chan time.Time {
	// TODO: handle d
	c := make(chan time.Time, 1)
	c <- this.now
	return c
}

// Tells mock clock about time progression
func (this *MockClock) NotifyTimeElapsed(d time.Duration) {
	//TODO: unblock all "due" channels, created by After() function
	this.now = this.Now().Add(d)
}
