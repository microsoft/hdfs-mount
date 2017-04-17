// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"log"
	"io"
)

var Info    *log.Logger
var Warning *log.Logger
var Error   *log.Logger
var Fatal   *log.Logger

func InitLogger(info, warning, err, fatal io.Writer) {
	Info = log.New(info, "INFO: ", log.Ldate | log.Ltime)
	Warning = log.New(warning, "Warning: ", log.Lshortfile | log.Ldate | log.Ltime)
	Error = log.New(err, "Error: ", log.Lshortfile | log.Ldate | log.Ltime)
	Fatal = log.New(fatal, "Fatal error: ", log.Llongfile | log.Ldate | log.Ltime)
}
