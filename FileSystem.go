// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"

	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
)

type FileSystem struct {
	MountPoint      string       // Path to the mount point on a local file system
	HdfsAccessor    HdfsAccessor // Interface to access HDFS
	AllowedPrefixes []string     // List of allowed path prefixes (only those prefixes are exposed via mountpoint)
	ExpandZips      bool         // Indicates whether ZIP expansion feature is enabled
	ReadOnly        bool         // Indicates whether mount filesystem with readonly
	Mounted         bool         // True if filesystem is mounted
	RetryPolicy     *RetryPolicy // Retry policy
	Clock           Clock        // interface to get wall clock time
	FsInfo          FsInfo       // Usage of HDFS, including capacity, remaining, used sizes.

	closeOnUnmount     []io.Closer // list of opened files (zip archives) to be closed on unmount
	closeOnUnmountLock sync.Mutex  // mutex to protet closeOnUnmount
}

// Verify that *FileSystem implements necesary FUSE interfaces
var _ fs.FS = (*FileSystem)(nil)
var _ fs.FSStatfser = (*FileSystem)(nil)

// Creates an instance of mountable file system
func NewFileSystem(hdfsAccessor HdfsAccessor, mountPoint string, allowedPrefixes []string, expandZips bool, readOnly bool, retryPolicy *RetryPolicy, clock Clock) (*FileSystem, error) {
	return &FileSystem{
		HdfsAccessor:    hdfsAccessor,
		MountPoint:      mountPoint,
		Mounted:         false,
		AllowedPrefixes: allowedPrefixes,
		ExpandZips:      expandZips,
		ReadOnly:        readOnly,
		RetryPolicy:     retryPolicy,
		Clock:           clock}, nil
}

// Mounts the filesystem
func (this *FileSystem) Mount() (*fuse.Conn, error) {
	var conn *fuse.Conn
	var err error
	//TODO: make configurable
	if this.ReadOnly {
		conn, err = fuse.Mount(
			this.MountPoint,
			fuse.FSName("hdfs"),
			fuse.Subtype("hdfs"),
			fuse.VolumeName("HDFS filesystem"),
			fuse.AllowOther(),
			fuse.WritebackCache(),
			fuse.MaxReadahead(1024*64),
			fuse.ReadOnly())
	} else {
		conn, err = fuse.Mount(
			this.MountPoint,
			fuse.FSName("hdfs"),
			fuse.Subtype("hdfs"),
			fuse.VolumeName("HDFS filesystem"),
			fuse.AllowOther(),
			fuse.WritebackCache(),
			fuse.MaxReadahead(1024*64))
	}
	if err != nil {
		return nil, err
	}
	this.Mounted = true
	return conn, nil
}

// Unmounts the filesysten (invokes fusermount tool)
func (this *FileSystem) Unmount() {
	if !this.Mounted {
		return
	}
	this.Mounted = false
	log.Print("Unmounting...")
	cmd := exec.Command("fusermount", "-zu", this.MountPoint)
	err := cmd.Run()

	// Closing all the files
	this.closeOnUnmountLock.Lock()
	defer this.closeOnUnmountLock.Unlock()
	for _, f := range this.closeOnUnmount {
		f.Close()
	}

	if err != nil {
		log.Fatal(err)
	}
}

// Returns root directory of the filesystem
func (this *FileSystem) Root() (fs.Node, error) {
	return &Dir{FileSystem: this, Attrs: Attrs{Inode: 1, Name: "", Mode: 0755 | os.ModeDir}}, nil
}

// Returns if given absoute path allowed by any of the prefixes
func (this *FileSystem) IsPathAllowed(path string) bool {
	if path == "/" {
		return true
	}
	for _, prefix := range this.AllowedPrefixes {
		if prefix == "*" {
			return true
		}
		p := "/" + prefix
		if p == path || strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	return false
}

// Register a file to be closed on Unmount()
func (this *FileSystem) CloseOnUnmount(file io.Closer) {
	this.closeOnUnmountLock.Lock()
	defer this.closeOnUnmountLock.Unlock()
	this.closeOnUnmount = append(this.closeOnUnmount, file)
}

// Statfs is called to obtain file system metadata.
// It should write that data to resp.
func (this *FileSystem) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	fsInfo, err := this.HdfsAccessor.StatFs()
	if err != nil {
		Warning.Println("Failed to get HDFS info,", err)
		return err
	}
	resp.Bsize = 1024
	resp.Bfree = fsInfo.remaining / uint64(resp.Bsize)
	resp.Bavail = resp.Bfree
	resp.Blocks = fsInfo.capacity / uint64(resp.Bsize)
	return nil
}
