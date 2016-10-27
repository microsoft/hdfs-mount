hdfs-mount
==========

[![Build Status](https://travis-ci.org/Microsoft/hdfs-mount.svg?branch=master)](https://travis-ci.org/Microsoft/hdfs-mount)

Allows to mount remote HDFS as a local Linux filesystem and allow arbitrary applications / shell scripts to access HDFS as normal files and directories in efficient and secure way.

Features (Planned)
------------------
* High performance
   * directly interfacing Linux kernel for FUSE and HDFS using protocol buffers (requires no JavaVM)
   * designed and optimized for throughput-intensive workloads (throughput is traded for latency whenever possible)
   * full streaming and automatic read-ahead support
   * concurrent operations
   * In-memory metadata caching (very fast ls!)
* High stability and robust failure-handling behavior
   * automatic retries and failover, all configurable
   * optional lazy mounting, before HDFS becomes available
* Support for both reads and writes
  * support for random writes [slow, but functionally correct]
  * support for file truncations
* Optionally expands ZIP archives with extracting content on demand
  * this provides an effective solution to "millions of small files on HDFS" problem
* CoreOS and Docker-friendly
  * optionally packagable as a statically-linked self-contained executable

Current state
-------------
"Alpha", under active development. Basic R/O scenarios, key R/O throughout optimizations and ZIP support are implemented and outperform existing HDFS/FUSE solutions.
If you want to use the component - come back in few weeks
If you want to help - contact authors

Building
--------
Ensure that you cloned the git repository recursively, since it contains submodules.
Run 'make' to build and 'make test' to run unit test.
Please use Go version at least 1.6beta2. This version contains bugfix for handling zip64 archives necessary for hdfs-mount to operate normally.

Other Platforms
---------------
It should be relatively easy to enable this working on MacOS and FreeBSD, since all underlying dependencies are MacOS and FreeBSD-ready. Very few changes are needed to the code to get it working on those platforms, but it is currently not a priority for authors. Contact authors if you want to help.
