// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"os"
	"time"
)

// Attributes common to the file/directory HDFS nodes
type Attrs struct {
	Inode   uint64
	Name    string
	Mode    os.FileMode
	Size    uint64
	Uid     uint32
	Gid     uint32
	Mtime   time.Time
	Ctime   time.Time
	Crtime  time.Time
	Expires time.Time // indicates when cached attribute information expires
}

// FsInfo provides information about HDFS
type FsInfo struct {
	capacity  uint64
	used      uint64
	remaining uint64
}

// Converts Attrs datastructure into FUSE represnetation
func (this *Attrs) Attr(a *fuse.Attr) error {
	a.Inode = this.Inode
	a.Mode = this.Mode
	if (a.Mode & os.ModeDir) == 0 {
		a.Size = this.Size
	}
	a.Uid = this.Uid
	a.Gid = this.Gid
	a.Mtime = this.Mtime
	a.Ctime = this.Ctime
	a.Crtime = this.Crtime
	return nil
}

// returns fuse.DirentType for this attributes (DT_Dir or DT_File)
func (this *Attrs) FuseNodeType() fuse.DirentType {
	if (this.Mode & os.ModeDir) == os.ModeDir {
		return fuse.DT_Dir
	} else {
		return fuse.DT_File
	}
}
