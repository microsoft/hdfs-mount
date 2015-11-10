// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"fmt"
	"log"
	"time"
)

// Encapsulats policy and logic of handling retries
type RetryPolicy struct {
	Clock       Clock         // Interface to clock
	MaxAttempts int           // Maximum allowed attempts for operations
	TimeLimit   time.Duration // Time limit for retries on subsequent failures
}

type Op struct {
	RetryPolicy *RetryPolicy // Pointed to the shared policy data structure
	Attempt     int          // 1-based index of current attemmpt
	Expires     time.Time    // point in time after which no retries are allowed
}

// Creates trivial retry policy which disallows all retries
func NewNoRetryPolicy() *RetryPolicy {
	return &RetryPolicy{MaxAttempts: 1, Clock: WallClock{}}
}

// Creates default retry policy
func NewDefaultRetryPolicy(clock Clock) *RetryPolicy {
	return &RetryPolicy{
		Clock:       clock,
		MaxAttempts: 3,
		TimeLimit:   5 * time.Minute}
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
		log.Printf(fmt.Sprintf("%s -> failed attempt #%d: will NOT be retried (%s)", message, op.Attempt, diag), args...)
		return false
	}
	// Computing delay (exponential backoff)
	delay := time.Second //TODO: implement exponential backoff

	// Logging information about failed attempt
	log.Printf(fmt.Sprintf("%s -> failed attempt #%d: retrying in %s", message, op.Attempt, delay), args...)
	op.Attempt++

	// Sleeping
	<-op.RetryPolicy.Clock.After(delay)

	// Allowing to retry
	return true
}
