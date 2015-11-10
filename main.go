// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s NAMENODE:PORT MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	lazyMount := flag.Bool("lazy", false, "Allows to mount HDFS filesystem before HDFS is available")

	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 2 {
		Usage()
		os.Exit(2)
	}

	retryPolicy := NewDefaultRetryPolicy(WallClock{})
	// TODO: add command line options to customize retry polic

	hdfsAccessor, err := NewHdfsAccessor(flag.Arg(0), retryPolicy, *lazyMount, WallClock{})
	if err != nil {
		log.Fatal("Error/NewHdfsAccessor: ", err)
	}

	fileSystem, err := NewFileSystem(hdfsAccessor, flag.Arg(1), WallClock{})
	if err != nil {
		log.Fatal("Error/NewFileSystem: ", err)
	}

	c, err := fileSystem.Mount()
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		fileSystem.Unmount()
		log.Print("Closing...")
		c.Close()
		log.Print("Closed...")
	}()

	go func() {
		for x := range sigs {
			//Handling INT/TERM signals - trying to gracefully unmount and exit
			//TODO: before doing that we need to finish deferred flushes
			log.Print("Signal received: " + x.String())
			fileSystem.Unmount() // this will cause Serve() call below to exit
			// Also reseting retry policy properties to stop useless retries
			retryPolicy.MaxAttempts = 0
			retryPolicy.MaxDelay = 0
		}
	}()
	err = fs.Serve(c, fileSystem)
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}
