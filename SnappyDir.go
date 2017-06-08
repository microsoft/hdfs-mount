// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"archive/zip"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"strings"
	"sync"
)

// Encapsulates state and operations for a directory inside a snappy file on HDFS file system
type SnappyDir struct {
	Attrs            Attrs               // Attributes of the directory
	SnappyContainerFile *File               // Zip container file node
	IsRoot           bool                // true if this ZipDir represents archive root
	SubDirs          map[string]*ZipDir  // Sub-directories (immediate children)
	Files            map[string]*ZipFile // Files in this directory
	ReadArchiveLock  sync.Mutex          // Used when reading the archive for root zip node (IsRoot==true)
}
