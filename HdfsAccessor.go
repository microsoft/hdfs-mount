// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"errors"
	"fmt"
	"github.com/colinmarc/hdfs"
	"github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"io"
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
	EnsureConnected() error                    // Ensures HDFS accessor is connected to the HDFS name node
	//TODO: mkdir, remove, etc...
}

type hdfsAccessorImpl struct {
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
func NewHdfsAccessor(nameNodeAddresses string, clock Clock) (HdfsAccessor, error) {
	nns := strings.Split(nameNodeAddresses, ",")

	this := &hdfsAccessorImpl{
		NameNodeAddresses:  nns,
		CurrentNameNodeIdx: 0,
		Clock:              clock,
		UserNameToUidCache: make(map[string]UidCacheEntry)}
	return this, nil
}

// Ensures that metadata client is connected
func (this *hdfsAccessorImpl) EnsureConnected() error {
	if this.MetadataClient != nil {
		return nil
	}
	return this.ConnectMetadataClient()
}

// Establishes connection to the name node (assigns MetadataClient field)
func (this *hdfsAccessorImpl) ConnectMetadataClient() error {
	client, err := this.ConnectToNameNode()
	if err != nil {
		return err
	}
	this.MetadataClient = client
	return nil
}

// Establishes connection to a name node in the context of some other operation
func (this *hdfsAccessorImpl) ConnectToNameNode() (*hdfs.Client, error) {
	// connecting to HDFS name node
	nnAddr := this.NameNodeAddresses[this.CurrentNameNodeIdx]
	client, err := this.connectToNameNodeImpl(nnAddr)
	if err != nil {
		// Connection failed, updating CurrentNameNodeIdx to try different name node next time
		this.CurrentNameNodeIdx = (this.CurrentNameNodeIdx + 1) % len(this.NameNodeAddresses)
		return nil, errors.New(fmt.Sprintf("%s: %s", nnAddr, err.Error()))
	}
	return client, nil
}

// Performs an attempt to connect to the HDFS name
func (this *hdfsAccessorImpl) connectToNameNodeImpl(nnAddr string) (*hdfs.Client, error) {
	// Performing an attempt to connect to the name node
	client, err := hdfs.New(nnAddr)
	if err != nil {
		return nil, err
	}
	// connection is OK, but we need to check whether name node is operating ans expected
	// (this also checks whether name node is Active)
	// Performing this check, by doing Stat() for a path inside root directory
	// Note: The file '/$' doesn't have to be present
	// - both nil and ErrNotExists error codes indicate success of the operation
	_, statErr := client.Stat("/$")

	if pathError, ok := statErr.(*os.PathError); statErr == nil || ok && (pathError.Err == os.ErrNotExist) {
		// Succesfully connected
		return client, nil
	} else {
		//TODO: how to close connection ?
		return nil, statErr
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
		//TODO: close connection
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
	if this.MetadataClient == nil {
		if err := this.ConnectMetadataClient(); err != nil {
			return nil, err
		}
	}
	files, err := this.MetadataClient.ReadDir(path)
	if err != nil {
		if IsSuccessOrBenignError(err) {
			// benign error (e.g. path not found)
			return nil, err
		}
		// We've got error from this client, setting to nil, so we try another one next time
		this.MetadataClient = nil
		// TODO: attempt to gracefully close the conenction
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

	if this.MetadataClient == nil {
		if err := this.ConnectMetadataClient(); err != nil {
			return Attrs{}, err
		}
	}

	fileInfo, err := this.MetadataClient.Stat(path)
	if err != nil {
		if IsSuccessOrBenignError(err) {
			// benign error (e.g. path not found)
			return Attrs{}, err
		}
		// We've got error from this client, setting to nil, so we try another one next time
		this.MetadataClient = nil
		// TODO: attempt to gracefully close the conenction
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

// Returns true if err==nil or err is expected (benign) error which should be propagated directoy to the caller
func IsSuccessOrBenignError(err error) bool {
	if err == nil || err == io.EOF {
		return true
	}
	if pathError, ok := err.(*os.PathError); ok && (pathError.Err == os.ErrNotExist) {
		return true
	} else {
		return false
	}
}
