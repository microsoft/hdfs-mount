// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"log"
	"os"
	"os/exec"
)

type FileSystem struct {
	MountPoint   string       // Path to the mount point on a local file system
	HdfsAccessor HdfsAccessor // Interface to access HDFS
	Mounted      bool         // True if filesystem is mounted
	Clock        Clock        // interface to get wall clock time
}

// Verify that *FileSystem implements necesary FUSE interfaces
var _ fs.FS = (*FileSystem)(nil)

// Creates an instance of mountable file system
func NewFileSystem(hdfsAccessor HdfsAccessor, mountPoint string, clock Clock) (*FileSystem, error) {
	return &FileSystem{HdfsAccessor: hdfsAccessor, MountPoint: mountPoint, Mounted: false, Clock: clock}, nil
}

// Mounts the filesystem
func (this *FileSystem) Mount() (*fuse.Conn, error) {
	conn, err := fuse.Mount(
		this.MountPoint,
		fuse.FSName("hdfs"),
		fuse.Subtype("hdfs"),
		fuse.VolumeName("HDFS filesystem"),
		fuse.MaxReadahead(1024*64)) //TODO: make configurable
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
	if err != nil {
		log.Fatal(err)
	}
}

// Returns root directory of the filesystem
func (this *FileSystem) Root() (fs.Node, error) {
	return &Dir{FileSystem: this, Attrs: Attrs{Inode: 1, Name: "", Mode: 0755 | os.ModeDir}}, nil
}
