// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"fmt"
	"math/rand"
	"time"
)

// Encapsulats policy and logic of handling retries
type RetryPolicy struct {
	Clock           Clock         // Interface to clock
	MaxAttempts     int           // Maximum allowed attempts for operations
	TimeLimit       time.Duration // Time limit for retries on subsequent failures
	MinDelay        time.Duration // minimum delay between retries (note, first retry always happens immediatelly)
	MaxDelay        time.Duration // maximum delay between retries
	RandomizeDelays bool          // true to randomize delays between retires
	ExpBackoffBase  float64       // base for the exponent function to compute delays between attempts
}

type Op struct {
	RetryPolicy *RetryPolicy  // Pointed to the shared policy data structure
	Attempt     int           // 1-based index of current attemmpt
	Expires     time.Time     // point in time after which no retries are allowed
	Delay       time.Duration // last delay (exponentially grows)
}

// Creates trivial retry policy which disallows all retries
func NewNoRetryPolicy() *RetryPolicy {
	return &RetryPolicy{MaxAttempts: 1, Clock: WallClock{}}
}

// Creates default retry policy.
// Default retry policy is time-based
// using randomized delay between 1sec-1min.
// The base for the exponential backoff is set as a golden ratio
// (delays grow approximatelly as the numbers in Fibonacci sequence)
func NewDefaultRetryPolicy(clock Clock) *RetryPolicy {
	return &RetryPolicy{
		Clock:           clock,
		MaxAttempts:     10,
		TimeLimit:       5 * time.Minute,
		MinDelay:        1 * time.Second,
		MaxDelay:        1 * time.Minute,
		RandomizeDelays: true,
		ExpBackoffBase:  1.618}
}

// Starts a new operation (a retry context) and returns data structure to track operation retires
func (retryPolicy *RetryPolicy) StartOperation() *Op {
	return &Op{
		Attempt:     1,
		RetryPolicy: retryPolicy,
		Expires:     retryPolicy.Clock.Now().Add(retryPolicy.TimeLimit)}
}

// Prints diagnostic message (using Printf formatting semantic) and
// returns true if retry should be performed for the failed operation.
// Before returing this function might sleep for some time, providing exponential backoff
func (op *Op) ShouldRetry(message string, args ...interface{}) bool {
	// Deciding whether to retry by # of attempts and time
	diag := ""
	if op.Attempt >= op.RetryPolicy.MaxAttempts {
		diag = "reached max # of attempts"
	} else if op.RetryPolicy.Clock.Now().After(op.Expires) {
		diag = "exceeded max configured time interval for retries"
	}
	if diag != "" {
		Error.Printf(fmt.Sprintf("%s -> failed attempt #%d: will NOT be retried (%s)", message, op.Attempt, diag), args...)
		return false
	}
	// Computing delay (exponential backoff)
	if op.Attempt == 2 {
		op.Delay = op.RetryPolicy.MinDelay
	} else if op.Attempt > 2 {
		op.Delay = time.Duration(float64(op.Delay) * op.RetryPolicy.ExpBackoffBase)
		if op.Delay > op.RetryPolicy.MaxDelay {
			op.Delay = op.RetryPolicy.MaxDelay
		}
	}

	effectiveDelay := op.Delay
	if op.RetryPolicy.RandomizeDelays && op.Delay > op.RetryPolicy.MinDelay {
		effectiveDelay = op.RetryPolicy.MinDelay + time.Duration(float64(op.Delay-op.RetryPolicy.MinDelay)*rand.Float64())
	}

	// Logging information about failed attempt
	Warning.Printf(fmt.Sprintf("%s -> failed attempt #%d: retrying in %s", message, op.Attempt, effectiveDelay), args...)
	op.Attempt++

	// Sleeping
	<-op.RetryPolicy.Clock.After(effectiveDelay)

	// Allowing to retry
	return true
}
