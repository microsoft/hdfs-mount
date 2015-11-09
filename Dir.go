// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"log"
	"os"
	"path"
	"sync"
)

// Encapsulates state and operations for directory node on the HDFS file system
type Dir struct {
	FileSystem   *FileSystem        // Pointer to the owning filesystem
	Attrs        Attrs              // Cached attributes of the directory, TODO: add TTL
	Parent       *Dir               // Pointer to the parent directory (allows computing fully-qualified paths on demand)
	Entries      map[string]fs.Node // Cahed directory entries
	EntriesMutex sync.Mutex         // Used to protect Entries
}

// Verify that *Dir implements necesary FUSE interfaces
var _ fs.Node = (*Dir)(nil)
var _ fs.HandleReadDirAller = (*Dir)(nil)
var _ fs.NodeStringLookuper = (*Dir)(nil)

// Retunds absolute path of the dir in HDFS namespace
func (this *Dir) AbsolutePath() string {
	if this.Parent == nil {
		return "/"
	} else {
		return path.Join(this.Parent.AbsolutePath(), this.Attrs.Name)
	}
}

// Responds on FUSE request to get directory attributes
func (this *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	return this.Attrs.Attr(a)
}

func (this *Dir) EntriesGet(name string) fs.Node {
	this.EntriesMutex.Lock()
	defer this.EntriesMutex.Unlock()
	if this.Entries == nil {
		this.Entries = make(map[string]fs.Node)
		return nil
	}
	return this.Entries[name]
}

func (this *Dir) EntriesSet(name string, node fs.Node) {
	this.EntriesMutex.Lock()
	defer this.EntriesMutex.Unlock()
	this.Entries[name] = node
}

// Responds on FUSE request to lookup the directory
func (this *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	//log.Printf("[%s]Lookup: %s", this.AbsolutePath(), name)
	if node := this.EntriesGet(name); node != nil {
		return node, nil
	}
	attrs, err := this.FileSystem.HdfsAccessor.Stat(path.Join(this.AbsolutePath(), name))
	if err != nil {
		log.Printf("Error/stat: %s %v", err.Error(), err)
		if pathError, ok := err.(*os.PathError); ok && (pathError.Err == os.ErrNotExist) {
			return nil, fuse.ENOENT
		}
		return nil, err
	}
	return this.NodeFromAttrs(attrs), nil
}

// Responds on FUSE request to read directory
func (this *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	if this.Entries == nil {
		this.Entries = make(map[string]fs.Node)
	}
	absolutePath := this.AbsolutePath()
	log.Printf("[%s]ReadDirAll", absolutePath)

	allAttrs, err := this.FileSystem.HdfsAccessor.ReadDir(absolutePath)
	if err != nil {
		log.Print("Error/ls: ", err)
		return nil, err
	}
	entries := make([]fuse.Dirent, len(allAttrs))
	for i, a := range allAttrs {
		// Creating Dirent structure as required by FUSE
		entries[i] = fuse.Dirent{
			Inode: a.Inode,
			Name:  a.Name,
			Type:  a.FuseNodeType()}
		// Speculatively pre-creating child Dir or File node with cached attributes,
		// since it's highly likely that we will have Lookup() call for this name
		// This is the key trick which dramatically speeds up 'ls'
		this.NodeFromAttrs(a)
	}
	return entries, nil
}

func (this *Dir) NodeFromAttrs(attrs Attrs) fs.Node {
	var node fs.Node
	if (attrs.Mode & os.ModeDir) == 0 {
		node = &File{FileSystem: this.FileSystem, Parent: this, Attrs: attrs}
	} else {
		node = &Dir{FileSystem: this.FileSystem, Parent: this, Attrs: attrs}
	}
	this.EntriesSet(attrs.Name, node)
	return node
}
