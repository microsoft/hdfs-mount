// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIsPathAllowedForStarPrefix(t *testing.T) {
	fs, _ := NewFileSystem(nil, "/tmp", []string{"*"}, false, WallClock{})
	assert.True(t, fs.IsPathAllowed("/"))
	assert.True(t, fs.IsPathAllowed("/foo"))
	assert.True(t, fs.IsPathAllowed("/foo/bar"))
}

func TestIsPathAllowedForMiscPrefixes(t *testing.T) {
	fs, _ := NewFileSystem(nil, "/tmp", []string{"foo", "bar", "baz/qux"}, false, WallClock{})
	assert.True(t, fs.IsPathAllowed("/"))
	assert.True(t, fs.IsPathAllowed("/foo"))
	assert.True(t, fs.IsPathAllowed("/bar"))
	assert.True(t, fs.IsPathAllowed("/foo/x"))
	assert.True(t, fs.IsPathAllowed("/baz/qux"))
	assert.True(t, fs.IsPathAllowed("/baz/qux/y"))
	assert.False(t, fs.IsPathAllowed("qux"))
	assert.False(t, fs.IsPathAllowed("w/z"))
}
