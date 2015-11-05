hdfs-mount
==========
Allows to  mount remote HDFS as a local Linux filesystem and allow arbitrary applications / shell scripts to access HDFS as normal files and directories in efficient way.

Features
--------
* High stability
* High performance
   * directly interfacing linux kernel for FUSE and HDFS using protocol buffers (requires no JavaVM)
   * designed to be used inr data-intensive workloads
   * optimized for throughput-sensitive workloads (throughput is traded for latency whenever possible)
   * full streaming and automatic read-ahead support
   * concurrent operations 
* Support for symbolic links
* Support for both reads and writes
  * Support for random writes [slow, but functionally correct]
  * Support for file trucations
* In-memory metadata caching (very fast ls)
* Customizable via command line options and configuraiton file
* CoreOS and Docker-friendly: packagable as a statically-linked self-contained executable

Current state
-------------
Prototype - under active development
