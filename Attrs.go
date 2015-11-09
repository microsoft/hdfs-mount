// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"os"
)

// Attributes common to the file/directory HDFS nodes
type Attrs struct {
	Inode uint64
	Name  string
	Mode  os.FileMode
	Size  uint64
}

// Converts Attrs datastructure into FUSE represnetation
func (this *Attrs) Attr(a *fuse.Attr) error {
	a.Inode = this.Inode
	a.Mode = this.Mode
	if (a.Mode & os.ModeDir) == 0 {
		a.Size = this.Size
	}
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
