// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"errors"
	"github.com/colinmarc/hdfs"
	"github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"os"
	"os/user"
	"strconv"
	"sync"
	"time"
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
	Clock               Clock                    // interface to get wall clock time
	NameNodeAddress     string                   // Address:port of the name node, TODO: allow specifying multiple addresses
	MetadataClient      *hdfs.Client             // HDFS client used for metadata operations
	MetadataClientMutex sync.Mutex               // Serializing all metadata operations for simplicity (for now), TODO: allow N concurrent operations
	UserNameToUidCache  map[string]UidCacheEntry // cache for converting usernames to UIDs
}

type UidCacheEntry struct {
	Uid     uint32    // User Id
	Expires time.Time // Absolute time when this cache entry expires
}

var _ HdfsAccessor = (*hdfsAccessorImpl)(nil) // ensure hdfsAccessorImpl implements HdfsAccessor

// Creates an instance of HdfsAccessor
func NewHdfsAccessor(nameNodeAddress string, clock Clock) (HdfsAccessor, error) {
	//TODO: support deferred on-demand creation to allow successful mounting before HDFS is available
	client, err := hdfs.New(nameNodeAddress)
	if err != nil {
		return nil, err
	}
	return &hdfsAccessorImpl{
			Clock:              clock,
			NameNodeAddress:    nameNodeAddress,
			MetadataClient:     client,
			UserNameToUidCache: make(map[string]UidCacheEntry)},
		nil
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
	for i, fileInfo := range files {
		allAttrs[i] = this.AttrsFromFileInfo(fileInfo)
	}
	return allAttrs, nil
}

// retrieves file/directory attributes
func (this *hdfsAccessorImpl) Stat(path string) (Attrs, error) {
	this.MetadataClientMutex.Lock()
	defer this.MetadataClientMutex.Unlock()

	fileInfo, err := this.MetadataClient.Stat(path)
	if err != nil {
		return Attrs{}, err
	}
	return this.AttrsFromFileInfo(fileInfo), nil
}

// Converts os.FileInfo + underlying proto-buf data into Attrs structure
func (this *hdfsAccessorImpl) AttrsFromFileInfo(fileInfo os.FileInfo) Attrs {
	protoBufData := fileInfo.Sys().(*hadoop_hdfs.HdfsFileStatusProto)
	mode := os.FileMode(*protoBufData.Permission.Perm)
	if fileInfo.IsDir() {
		mode |= os.ModeDir
	}
	return Attrs{
		Inode: *protoBufData.FileId,
		Name:  fileInfo.Name(),
		Mode:  mode,
		Size:  *protoBufData.Length,
		Uid:   this.LookupUid(*protoBufData.Owner),
		Gid:   0} // TODO: Group is now hardcoded to be "root", implement proper mapping
}

// Performs a cache-assisted lookup of UID by username
func (this *hdfsAccessorImpl) LookupUid(userName string) uint32 {
	if userName == "" {
		return 0
	}
	// Note: this method is called under MetadataClientMutex, so accessing the cache dirctionary is safe
	cacheEntry, ok := this.UserNameToUidCache[userName]
	if ok && this.Clock.Now().Before(cacheEntry.Expires) {
		return cacheEntry.Uid
	}
	print("u:" + userName + "\n")
	u, err := user.Lookup(userName)
	var uid64 uint64
	if err == nil {
		// UID is returned as string, need to parse it
		uid64, err = strconv.ParseUint(u.Uid, 10, 32)
	}
	if err != nil {
		uid64 = (1 << 31) - 1
	}
	this.UserNameToUidCache[userName] = UidCacheEntry{
		Uid:     uint32(uid64),
		Expires: this.Clock.Now().Add(5 * time.Minute)} // caching UID for 5 minutes
	return uint32(uid64)
}
