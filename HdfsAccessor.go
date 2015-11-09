// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"errors"
	"github.com/colinmarc/hdfs"
	"github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"os"
	"sync"
)

// Interface for accessing HDFS
// Concurrency: thread safe: handles unlimited number of concurrent requests
type HdfsAccessor interface {
	OpenRead(path string) (HdfsReader, error)  // Opens HDFS file for reading
	OpenWrite(path string) (HdfsWriter, error) // Opens HDFS file for writing
	ReadDir(path string) ([]Attrs, error)      // Enumerates HDFS directory
	Stat(path string) (Attrs, error)           // retrieves file/directory attributes
	//TODO: mkdir, remove, etc...
}

type hdfsAccessorImpl struct {
	NameNodeAddress     string       // Address:port of the name node, TODO: allow specifying multiple addresses
	MetadataClient      *hdfs.Client // HDFS client used for metadata operations
	MetadataClientMutex sync.Mutex   // Serializing all metadata operations for simplicity (for now), TODO: allow N concurrent operations
}

var _ HdfsAccessor = (*hdfsAccessorImpl)(nil) // ensure hdfsAccessorImpl implements HdfsAccessor

// Creates an instance of HdfsAccessor
func NewHdfsAccessor(nameNodeAddress string) (HdfsAccessor, error) {
	//TODO: support deferred on-demand creation to allow successful mounting before HDFS is available
	client, err := hdfs.New(nameNodeAddress)
	if err != nil {
		return nil, err
	}
	return &hdfsAccessorImpl{NameNodeAddress: nameNodeAddress, MetadataClient: client}, nil
}

// Opens HDFS file for reading
func (this *hdfsAccessorImpl) OpenRead(path string) (HdfsReader, error) {
	client, err1 := hdfs.New(this.NameNodeAddress)
	if err1 != nil {
		return nil, err1
	}
	reader, err2 := client.Open(path)
	if err2 != nil {
		return nil, err2
	}
	return NewHdfsReader(reader), nil
}

// Opens HDFS file for writing
func (this *hdfsAccessorImpl) OpenWrite(path string) (HdfsWriter, error) {
	return nil, errors.New("OpenWrite is not implemented")
}

// Enumerates HDFS directory
func (this *hdfsAccessorImpl) ReadDir(path string) ([]Attrs, error) {
	this.MetadataClientMutex.Lock()
	defer this.MetadataClientMutex.Unlock()

	files, err := this.MetadataClient.ReadDir(path)
	if err != nil {
		return nil, err
	}

	allAttrs := make([]Attrs, len(files))
	for i, f := range files {
		protoBufData := f.Sys().(*hadoop_hdfs.HdfsFileStatusProto)
		mode := os.FileMode(*protoBufData.Permission.Perm)
		if f.IsDir() {
			mode |= os.ModeDir
		}
		allAttrs[i] = Attrs{
			Inode: *protoBufData.FileId,
			Name:  f.Name(),
			Mode:  mode,
			Size:  *protoBufData.Length}
	}
	return allAttrs, nil
}

// retrieves file/directory attributes
func (this *hdfsAccessorImpl) Stat(path string) (Attrs, error) {
	this.MetadataClientMutex.Lock()
	defer this.MetadataClientMutex.Unlock()

	st, err := this.MetadataClient.Stat(path)
	if err != nil {
		return Attrs{}, err
	}
	attrs := Attrs{Inode: *st.Sys().(*hadoop_hdfs.HdfsFileStatusProto).FileId, Name: st.Name(), Mode: st.Mode(), Size: uint64(st.Size())}
	if st.IsDir() {
		attrs.Mode |= os.ModeDir
	}
	return attrs, nil
}
