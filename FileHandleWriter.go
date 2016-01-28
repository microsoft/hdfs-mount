// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"golang.org/x/net/context"
	"log"
	"os"
)

// Encapsulates state and routines for writing data from the file handle
type FileHandleWriter struct {
	Handle      *FileHandle
	stagingFile *os.File
	//HdfsWriter HdfsWriter // Backend writer
}

// Opens the file for writing
func NewFileHandleWriter(handle *FileHandle, newFile bool) (*FileHandleWriter, error) {
	this := &FileHandleWriter{Handle: handle}
	log.Printf("newFile=%v", newFile)
	if newFile {
		c := this.Handle.File.FileSystem.HdfsAccessor.(*FaultTolerantHdfsAccessor).Impl.(*hdfsAccessorImpl).MetadataClient
		c.Remove(this.Handle.File.AbsolutePath())
		w, err := c.CreateFile(this.Handle.File.AbsolutePath(), 1, 1*1024*1024, this.Handle.File.Attrs.Mode)
		if err != nil {
			log.Printf("ERROR creating %s: %s", this.Handle.File.AbsolutePath(), err)
			return nil, err
		}
		w.Close()

		this.stagingFile, err = os.OpenFile("/tmp/stage", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return nil, err
		}
	} else {
		// Request to write to existing file
		log.Printf("Buffering contents of the file to the staging area...")
		var err error
		this.stagingFile, err = os.OpenFile("/tmp/stage", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return nil, err
		}
	}
	os.Remove("/tmp/stage") //TODO: err
	return this, nil
}

// Responds on FUSE Write request
func (this *FileHandleWriter) Write(handle *FileHandle, ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	nw, err := this.stagingFile.WriteAt(req.Data, req.Offset)
	log.Printf("write @%d, %d, %d  %s", req.Offset, len(req.Data), nw, err)
	resp.Size = nw
	if err != nil {
		return err
	}
	return nil
}

// Responds on FUSE Flush/Fsync request
func (this *FileHandleWriter) Flush() error {
	log.Printf("[%s] flush", this.Handle.File.AbsolutePath())

	c := this.Handle.File.FileSystem.HdfsAccessor.(*FaultTolerantHdfsAccessor).Impl.(*hdfsAccessorImpl).MetadataClient
	c.Remove(this.Handle.File.AbsolutePath())
	w, err := c.CreateFile(this.Handle.File.AbsolutePath(), 1, 1*1024*1024, this.Handle.File.Attrs.Mode)
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

		w, err := w.Write(b)
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
