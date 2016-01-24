 # Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for details.

export GOPATH=$(PWD)/_gopath

all: hdfs-mount 

hdfs-mount: *.go $(GOPATH)/src/bazil.org/fuse $(GOPATH)/src/github.com/colinmarc/hdfs $(GOPATH)/src/golang.org/x/net/context $(GOPATH)/src/github.com/golang/protobuf/proto
	go build

$(GOPATH)/src/bazil.org/fuse: $(GOPATH)/src/github.com/bazil/fuse
	ln -s $(GOPATH)/src/github.com/bazil $(GOPATH)/src/bazil.org

$(GOPATH)/src/github.com/colinmarc/hdfs:
	mkdir -p $(shell dirname $@)
	ln -s ../../../../submodules/colinmarc-hdfs $@

$(GOPATH)/src/github.com/bazil/fuse:
	go get github.com/bazil/fuse || [ -f $(GOPATH)/src/github.com/bazil/fuse/fuse.go ] && echo Ignore the error above - this is expected

$(GOPATH)/src/%:
	go get $*

MOCKGEN_DIR=$(GOPATH)/src/github.com/golang/mock/mockgen

$(MOCKGEN_DIR)/mockgen.go:
	go get github.com/golang/mock/mockgen

$(MOCKGEN_DIR)/mockgen: $(MOCKGEN_DIR)/mockgen.go
	cd $(MOCKGEN_DIR) && go build
	ls -la $(MOCKGEN_DIR)/mockgen

clean:
	rm -f hdfs-mount _mock_*.go

mock_%_test.go: %.go | $(MOCKGEN_DIR)/mockgen
	$(MOCKGEN_DIR)/mockgen -source $< -package main > $@~
	mv -f $@~ $@

test: hdfs-mount \
	  $(GOPATH)/src/github.com/stretchr/testify/assert \
      $(GOPATH)/src/github.com/golang/mock/gomock \
      $(MOCKGEN_DIR)/mockgen \
      mock_HdfsAccessor_test.go \
      mock_ReadSeekCloser_test.go \
      mock_HdfsWriter_test.go
	go test
