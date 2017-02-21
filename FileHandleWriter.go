// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"errors"
	"golang.org/x/net/context"
	"io"
	"io/ioutil"
	"log"
	"os"
)

const MaxFileSizeForWrite uint64 = 200 * 1024 * 1024 * 1024 // 200G is a limit for now

// Encapsulates state and routines for writing data from the file handle
type FileHandleWriter struct {
	Handle       *FileHandle
	stagingFile  *os.File
	BytesWritten uint64
}

// Opens the file for writing
func NewFileHandleWriter(handle *FileHandle, newFile bool) (*FileHandleWriter, error) {
	this := &FileHandleWriter{Handle: handle}
	log.Printf("newFile=%v", newFile)
	path := this.Handle.File.AbsolutePath()

	hdfsAccessor := this.Handle.File.FileSystem.HdfsAccessor
	if newFile {
		hdfsAccessor.Remove(path)
		w, err := hdfsAccessor.CreateFile(path, this.Handle.File.Attrs.Mode)
		if err != nil {
			log.Printf("ERROR creating %s: %s", path, err)
			return nil, err
		}
		w.Close()
	}
	stageDir := "/var/hdfs-mount" // TODO: make configurable
	os.MkdirAll(stageDir, 0700)
	var err error
	this.stagingFile, err = ioutil.TempFile(stageDir, "stage")
	if err != nil {
		return nil, err
	}
	os.Remove(this.stagingFile.Name()) //TODO: handle error

	if !newFile {
		// Request to write to existing file
		attrs, err := hdfsAccessor.Stat(path)
		if err != nil {
			log.Printf("[%s] Can't stat file: %s", path, err)
			return this, nil
		}
		if attrs.Size >= MaxFileSizeForWrite {
			this.stagingFile.Close()
			this.stagingFile = nil
			log.Printf("[%s] Maximum allowed file size for writing exceeded (%d >= %d)", path, attrs.Size, MaxFileSizeForWrite)
			return nil, errors.New("Too large file")
		}

		log.Printf("Buffering contents of the file to the staging area %s...", this.stagingFile.Name())
		reader, err := hdfsAccessor.OpenRead(path)
		if err != nil {
			log.Printf("HDFS/open failure: %s", err)
			this.stagingFile.Close()
			this.stagingFile = nil
			return nil, err
		}
		nc, err := io.Copy(this.stagingFile, reader)
		if err != nil {
			log.Printf("Copy failure: %s", err)
			this.stagingFile.Close()
			this.stagingFile = nil
			return nil, err
		}
		log.Printf("Copied %d bytes", nc)
	}

	return this, nil
}

// Responds on FUSE Write request
func (this *FileHandleWriter) Write(handle *FileHandle, ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if uint64(req.Offset) >= MaxFileSizeForWrite {
		log.Printf("[%s] Maximum allowed file size for writing exceeded (%d >= %d)", this.Handle.File.AbsolutePath(), req.Offset, MaxFileSizeForWrite)
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
	log.Printf("[%s] flush (%d new bytes written)", this.Handle.File.AbsolutePath(), this.BytesWritten)
	if this.BytesWritten == 0 {
		// Nothing to do
		return nil
	}
	this.BytesWritten = 0
	defer this.Handle.File.InvalidateMetadataCache()

	op := this.Handle.File.FileSystem.RetryPolicy.StartOperation()
	for {
		err := this.FlushAttempt()
		if IsSuccessOrBenignError(err) || !op.ShouldRetry("[%s] Flush()", err) {
			return err
		}
	}
	return nil
}

// Single attempt to flush a file
func (this *FileHandleWriter) FlushAttempt() error {
	hdfsAccessor := this.Handle.File.FileSystem.HdfsAccessor
	hdfsAccessor.Remove(this.Handle.File.AbsolutePath())
	w, err := hdfsAccessor.CreateFile(this.Handle.File.AbsolutePath(), this.Handle.File.Attrs.Mode)
	if err != nil {
		log.Printf("ERROR creating %s: %s", this.Handle.File.AbsolutePath(), err)
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
			log.Printf("ERROR closing %s: %s", this.Handle.File.AbsolutePath(), err)
			w.Close()
			return err
		}

	}
	err = w.Close()
	if err != nil {
		log.Printf("ERROR closing %s: %s", this.Handle.File.AbsolutePath(), err)
		return err
	}

	return nil
}

// Closes the writer
func (this *FileHandleWriter) Close() error {
	return this.stagingFile.Close()
}
