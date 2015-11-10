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
