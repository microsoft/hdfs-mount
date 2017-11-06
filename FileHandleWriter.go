// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"errors"
	"golang.org/x/net/context"
	"io"
	"io/ioutil"
	"os"
)

// Encapsulates state and routines for writing data from the file handle
type FileHandleWriter struct {
	Handle       *FileHandle
	stagingFile  *os.File
	BytesWritten uint64
}

// Opens the file for writing
func NewFileHandleWriter(handle *FileHandle, newFile bool) (*FileHandleWriter, error) {
	this := &FileHandleWriter{Handle: handle}
	Info.Println("newFile=", newFile)
	path := this.Handle.File.AbsolutePath()

	hdfsAccessor := this.Handle.File.FileSystem.HdfsAccessor
	if newFile {
		hdfsAccessor.Remove(path)
		w, err := hdfsAccessor.CreateFile(path, this.Handle.File.Attrs.Mode)
		if err != nil {
			Error.Println("Creating", path, ":", path, err)
			return nil, err
		}
		w.Close()
	}
	stageDir := "/var/hdfs-mount" // TODO: make configurable
	if ok := os.MkdirAll(stageDir, 0700); ok != nil {
		Error.Println("Failed to create stageDir /var/hdfs-mount, Error:", ok)
		return nil, ok
	}
	var err error
	this.stagingFile, err = ioutil.TempFile(stageDir, "stage")
	if err != nil {
		return nil, err
	}
	os.Remove(this.stagingFile.Name()) //TODO: handle error

	if !newFile {
		// Request to write to existing file
		_, err := hdfsAccessor.Stat(path)
		if err != nil {
			Warning.Println("[", path, "] Can't stat file:", err)
			return this, nil
		}

		Info.Println("Buffering contents of the file to the staging area ", this.stagingFile.Name())
		reader, err := hdfsAccessor.OpenRead(path)
		if err != nil {
			Warning.Println("HDFS/open failure:", err)
			this.stagingFile.Close()
			this.stagingFile = nil
			return nil, err
		}
		nc, err := io.Copy(this.stagingFile, reader)
		if err != nil {
			Warning.Println("Copy failure:", err)
			this.stagingFile.Close()
			this.stagingFile = nil
			return nil, err
		}
		reader.Close()
		Info.Println("Copied", nc, "bytes")
	}

	return this, nil
}

// Responds on FUSE Write request
func (this *FileHandleWriter) Write(handle *FileHandle, ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	fsInfo, err := this.Handle.File.FileSystem.HdfsAccessor.StatFs()
	if err != nil {
		// Donot abort, continue writing
		Error.Println("Failed to get HDFS usage, ERROR:", err)
	} else if uint64(req.Offset) >= fsInfo.remaining {
		Error.Println("[", this.Handle.File.AbsolutePath(), "] writes larger size (", req.Offset, ")than HDFS available size (", fsInfo.remaining, ")")
		return errors.New("Too large file")
	}

	nw, err := this.stagingFile.WriteAt(req.Data, req.Offset)
	resp.Size = nw
	if err != nil {
		return err
	}
	this.BytesWritten += uint64(nw)
	return nil
}

// Responds on FUSE Flush/Fsync request
func (this *FileHandleWriter) Flush() error {
	Info.Println("[", this.Handle.File.AbsolutePath(), "] flush (", this.BytesWritten, "new bytes written)")
	if this.BytesWritten == 0 {
		// Nothing to do
		return nil
	}
	this.BytesWritten = 0
	defer this.Handle.File.InvalidateMetadataCache()

	op := this.Handle.File.FileSystem.RetryPolicy.StartOperation()
	for {
		err := this.FlushAttempt()
		if err != io.EOF || IsSuccessOrBenignError(err) || !op.ShouldRetry("Flush()", err) {
			return err
		}
		// Restart a new connection, https://github.com/colinmarc/hdfs/issues/86
		this.Handle.File.FileSystem.HdfsAccessor.Close()
		Error.Println("[", this.Handle.File.AbsolutePath(), "] failed flushing. Retry")
	}
	return nil
}

// Single attempt to flush a file
func (this *FileHandleWriter) FlushAttempt() error {
	hdfsAccessor := this.Handle.File.FileSystem.HdfsAccessor
	hdfsAccessor.Remove(this.Handle.File.AbsolutePath())
	w, err := hdfsAccessor.CreateFile(this.Handle.File.AbsolutePath(), this.Handle.File.Attrs.Mode)
	if err != nil {
		Error.Println("ERROR creating", this.Handle.File.AbsolutePath(), ":", err)
		return err
	}

	this.stagingFile.Seek(0, 0)
	b := make([]byte, 65536, 65536)
	for {
		nr, err := this.stagingFile.Read(b)
		if err != nil {
			break
		}
		b = b[:nr]

		_, err = w.Write(b)
		if err != nil {
			Error.Println("Closing", this.Handle.File.AbsolutePath(), ":", err)
			w.Close()
			return err
		}

	}
	err = w.Close()
	if err != nil {
		Error.Println("Closing", this.Handle.File.AbsolutePath(), ":", err)
		return err
	}

	return nil
}

// Closes the writer
func (this *FileHandleWriter) Close() error {
	return this.stagingFile.Close()
}
