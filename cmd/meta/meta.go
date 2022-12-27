// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// binary entry point for the meta service
package main

import (
	"github.com/tatris-io/tatris/internal/meta"
	"github.com/tatris-io/tatris/internal/service"
)

func main() {
	meta.SayHello()
	service.StartHTTPServer("meta")
	println("init project for meta")
}
