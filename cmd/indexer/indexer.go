// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// binary entry point for indexer
package main

import (
	"github.com/tatris-io/tatris/internal/ingestion"
	"github.com/tatris-io/tatris/internal/service"
)

func main() {
	ingestion.SayHello()
	service.StartHTTPServer("ingestion")
	println("init project for indexer")
}
