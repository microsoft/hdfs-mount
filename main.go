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
	"time"
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
	retryTimeLimit := flag.Duration("retryTimeLimit", 5*time.Minute, "time limit for all retry attempts for failed operations")
	retryMaxAttempts := flag.Uint("retryMaxAttempts", 99999999, "Maxumum retry attempts for failed operations")
	retryMinDelay := flag.Duration("retryMinDelay", 1*time.Second, "minimum delay between retries (note, first retry always happens immediatelly)")
	retryMaxDelay := flag.Duration("retryMaxDelay", 60*time.Second, "maximum delay between retries")

	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 2 {
		Usage()
		os.Exit(2)
	}

	retryPolicy := NewDefaultRetryPolicy(WallClock{})
	retryPolicy.TimeLimit = *retryTimeLimit
	retryPolicy.MaxAttempts = int(*retryMaxAttempts) + 1 // converting # of retry attempts to total # of attempts
	retryPolicy.MinDelay = *retryMinDelay
	retryPolicy.MaxDelay = *retryMaxDelay

	hdfsAccessor, err := NewHdfsAccessor(flag.Arg(0), WallClock{})
	if err != nil {
		log.Fatal("Error/NewHdfsAccessor: ", err)
	}

	// Wrapping with FaultTolerantHdfsAccessor
	ftHdfsAccessor := NewFaultTolerantHdfsAccessor(hdfsAccessor, retryPolicy)

	if !*lazyMount && ftHdfsAccessor.EnsureConnected() != nil {
		log.Fatal("Can't establish connection to HDFS, mounting will NOT be performend (this can be suppressed with -lazy)")
	}

	// Creating the virtual file system
	fileSystem, err := NewFileSystem(ftHdfsAccessor, flag.Arg(1), WallClock{})
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
