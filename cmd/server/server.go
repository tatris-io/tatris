// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// binary entry point for the standalone mode
package main

import (
	"github.com/alecthomas/kong"
	"github.com/tatris-io/tatris/cmd"
	"github.com/tatris-io/tatris/internal/service"
)

var cli cmd.Cli

func main() {
	kong.Parse(&cli)
	cmd.Initialize(&cli)
	service.StartHTTPServer("all")
}
