// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"os"
)

// Adds automatic retry capability to HdfsAccessor with respect to RetryPolicy
type FaultTolerantHdfsAccessor struct {
	Impl        HdfsAccessor
	RetryPolicy *RetryPolicy
}

var _ HdfsAccessor = (*FaultTolerantHdfsAccessor)(nil) // ensure FaultTolerantHdfsAccessor implements HdfsAccessor

// Creates an instance of FaultTolerantHdfsAccessor
func NewFaultTolerantHdfsAccessor(impl HdfsAccessor, retryPolicy *RetryPolicy) *FaultTolerantHdfsAccessor {
	return &FaultTolerantHdfsAccessor{
		Impl:        impl,
		RetryPolicy: retryPolicy}
}

// Ensures HDFS accessor is connected to the HDFS name node
func (this *FaultTolerantHdfsAccessor) EnsureConnected() error {
	op := this.RetryPolicy.StartOperation()
	for {
		err := this.Impl.EnsureConnected()
		if IsSuccessOrBenignError(err) || !op.ShouldRetry("Connect: %s", err) {
			return err
		}
	}
}

// Opens HDFS file for reading
func (this *FaultTolerantHdfsAccessor) OpenRead(path string) (ReadSeekCloser, error) {
	op := this.RetryPolicy.StartOperation()
	for {
		result, err := this.Impl.OpenRead(path)
		if err == nil {
			// wrapping returned HdfsReader with FaultTolerantHdfsReader
			return NewFaultTolerantHdfsReader(path, result, this.Impl, this.RetryPolicy), nil
		}
		if IsSuccessOrBenignError(err) || !op.ShouldRetry("[%s] OpenRead: %s", path, err) {
			return nil, err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			this.Impl.Close()
		}
	}
}

// Opens HDFS file for writing
func (this *FaultTolerantHdfsAccessor) CreateFile(path string, mode os.FileMode) (HdfsWriter, error) {
	// TODO: implement fault-tolerance. For now re-try-loop is implemented inside FileHandleWriter
	return this.Impl.CreateFile(path, mode)
}

// Enumerates HDFS directory
func (this *FaultTolerantHdfsAccessor) ReadDir(path string) ([]Attrs, error) {
	op := this.RetryPolicy.StartOperation()
	for {
		result, err := this.Impl.ReadDir(path)
		if IsSuccessOrBenignError(err) || !op.ShouldRetry("[%s] ReadDir: %s", path, err) {
			return result, err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			this.Impl.Close()
		}
	}
}

// Retrieves file/directory attributes
func (this *FaultTolerantHdfsAccessor) Stat(path string) (Attrs, error) {
	op := this.RetryPolicy.StartOperation()
	for {
		result, err := this.Impl.Stat(path)
		if IsSuccessOrBenignError(err) || !op.ShouldRetry("[%s] Stat: %s", path, err) {
			return result, err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			this.Impl.Close()
		}
	}
}

// Retrieves HDFS usage
func (this *FaultTolerantHdfsAccessor) StatFs() (FsInfo, error) {
	op := this.RetryPolicy.StartOperation()
	for {
		result, err := this.Impl.StatFs()
		if IsSuccessOrBenignError(err) || !op.ShouldRetry("StatFs: %s", err) {
			return result, err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			this.Impl.Close()
		}
	}
}

// Creates a directory
func (this *FaultTolerantHdfsAccessor) Mkdir(path string, mode os.FileMode) error {
	op := this.RetryPolicy.StartOperation()
	for {
		err := this.Impl.Mkdir(path, mode)
		if IsSuccessOrBenignError(err) || !op.ShouldRetry("[%s] Mkdir %s: %s", path, mode, err) {
			return err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			this.Impl.Close()
		}
	}
}

// Removes a file or directory
func (this *FaultTolerantHdfsAccessor) Remove(path string) error {
	op := this.RetryPolicy.StartOperation()
	for {
		err := this.Impl.Remove(path)
		if IsSuccessOrBenignError(err) || !op.ShouldRetry("[%s] Remove: %s", path, err) {
			return err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			this.Impl.Close()
		}
	}
}

// Renames file or directory
func (this *FaultTolerantHdfsAccessor) Rename(oldPath string, newPath string) error {
	op := this.RetryPolicy.StartOperation()
	for {
		err := this.Impl.Rename(oldPath, newPath)
		if IsSuccessOrBenignError(err) || !op.ShouldRetry("[%s] Rename to %s: %s", oldPath, newPath, err) {
			return err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			this.Impl.Close()
		}
	}
}

// Chmod file or directory
func (this *FaultTolerantHdfsAccessor) Chmod(path string, mode os.FileMode) error {
	op := this.RetryPolicy.StartOperation()
	for {
		err := this.Impl.Chmod(path, mode)
		if IsSuccessOrBenignError(err) || !op.ShouldRetry("Chmod [%s] to [%d]: %s", path, mode, err) {
			return err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			this.Impl.Close()
		}
	}
}

// Chown file or directory
func (this *FaultTolerantHdfsAccessor) Chown(path string, user, group string) error {
	op := this.RetryPolicy.StartOperation()
	for {
		err := this.Impl.Chown(path, user, group)
		if IsSuccessOrBenignError(err) || !op.ShouldRetry("Chown [%s] to [%s:%s]: %s", path, user, group, err) {
			return err
		} else {
			// Clean up the bad connection, to let underline connection to get automatic refresh
			this.Impl.Close()
		}
	}
}

// Close underline connection if needed
func (this *FaultTolerantHdfsAccessor) Close() error {
	return this.Impl.Close()
}
