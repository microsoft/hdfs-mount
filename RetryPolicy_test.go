// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNoRetryPolicy(t *testing.T) {
	assert.False(t, NewNoRetryPolicy().StartOperation().ShouldRetry("TestDiagnostic"))
}

func TestTreeAttempts(t *testing.T) {
	rp := NewDefaultRetryPolicy(&MockClock{})
	rp.MaxAttempts = 3
	op := rp.StartOperation()
	assert.True(t, op.ShouldRetry("Attempt 1"))
	assert.True(t, op.ShouldRetry("Attempt 2"))
	assert.False(t, op.ShouldRetry("Attempt 3"))
}

func TestTreeMinutesLimit(t *testing.T) {
	clock := &MockClock{}
	rp := NewDefaultRetryPolicy(clock)
	rp.MaxAttempts = 9999999
	rp.TimeLimit = 3 * time.Minute
	op := rp.StartOperation()
	assert.True(t, op.ShouldRetry("Attempt 1"))
	clock.NotifyTimeElapsed(time.Minute)
	assert.True(t, op.ShouldRetry("Attempt 2"))
	clock.NotifyTimeElapsed(time.Minute)
	assert.True(t, op.ShouldRetry("Attempt 3"))
	clock.NotifyTimeElapsed(61 * time.Second)
	assert.False(t, op.ShouldRetry("Attempt 4"))
}

func TestExponentialBackoff(t *testing.T) {
	clock := &MockClock{}
	rp := NewDefaultRetryPolicy(clock)
	rp.MaxAttempts = 9999999
	rp.MinDelay = time.Second
	rp.MaxDelay = time.Minute
	rp.TimeLimit = time.Hour
	rp.RandomizeDelays = false
	op := rp.StartOperation()
	assert.True(t, op.ShouldRetry("Attempt 1"))
	assert.Equal(t, time.Duration(0), clock.LastSleepDuration) // first retry is immediate
	assert.True(t, op.ShouldRetry("Attempt 2"))
	assert.Equal(t, time.Second, clock.LastSleepDuration) // MinDelay
	assert.True(t, op.ShouldRetry("Attempt 3"))
	assert.InEpsilon(t, 1.62, clock.LastSleepDuration.Seconds(), 0.01)
	assert.True(t, op.ShouldRetry("Attempt 4"))
	assert.InEpsilon(t, 2.62, clock.LastSleepDuration.Seconds(), 0.01)
	assert.True(t, op.ShouldRetry("Attempt 5"))
	assert.InEpsilon(t, 4.24, clock.LastSleepDuration.Seconds(), 0.01)
	for i := 0; i < 7; i++ {
		assert.True(t, op.ShouldRetry("Attempt X"))
	}
	assert.Equal(t, time.Minute, clock.LastSleepDuration) // MaxDelay
}
