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
	"strings"
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
	RetryPolicy         *RetryPolicy             // pointer to the retry policy
	Clock               Clock                    // interface to get wall clock time
	NameNodeAddresses   []string                 // array of Address:port string for the name nodes
	CurrentNameNodeIdx  int                      // Index of the current name node in NameNodeAddresses array
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
func NewHdfsAccessor(nameNodeAddresses string, retryPolicy *RetryPolicy, clock Clock) (HdfsAccessor, error) {
	nns := strings.Split(nameNodeAddresses, ",")

	this := &hdfsAccessorImpl{
		NameNodeAddresses:  nns,
		CurrentNameNodeIdx: 0,
		RetryPolicy:        retryPolicy,
		Clock:              clock,
		UserNameToUidCache: make(map[string]UidCacheEntry)}

	//TODO: support deferred on-demand creation to allow successful mounting before HDFS is available
	client, err := this.ConnectToNameNode()
	if err != nil {
		return nil, err
	}
	this.MetadataClient = client
	return this, nil
}

func (this *hdfsAccessorImpl) ConnectToNameNode() (*hdfs.Client, error) {
	op := this.RetryPolicy.StartOperation()
	startIdx := this.CurrentNameNodeIdx
	for {
		// connecting to HDFS name nodes in round-robin fashion:
		nnIdx := (startIdx + op.Attempt - 1) % len(this.NameNodeAddresses)
		nnAddr := this.NameNodeAddresses[nnIdx]

		// Performing an attempt to connect to the name node
		client, err := hdfs.New(nnAddr)
		if err != nil {
			if op.ShouldRetry("connect %s: %s", nnAddr, err) {
				continue
			} else {
				return nil, err
			}
		}
		// connection is OK, but we need to check whether name node is operating ans expected
		// (this also checks whether name node is Active)
		// Performing this check, by doing Stat() for a path inside root directory
		// Note: The file '/$' doesn't have to be present
		// - both nil and ErrNotExists error codes indicate success of the operation
		_, statErr := client.Stat("/$")

		if pathError, ok := statErr.(*os.PathError); statErr == nil || ok && (pathError.Err == os.ErrNotExist) {
			// Succesfully connected, memoizing the index of the name node, to speedup next connect
			this.CurrentNameNodeIdx = nnIdx
			return client, nil
		} else {
			//TODO: how to close connection ?
			if op.ShouldRetry("healthcheck %s: %s", nnAddr, statErr) {
				continue
			} else {
				return nil, statErr
			}
		}
	}
}

// Opens HDFS file for reading
func (this *hdfsAccessorImpl) OpenRead(path string) (HdfsReader, error) {
	client, err1 := this.ConnectToNameNode()
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
