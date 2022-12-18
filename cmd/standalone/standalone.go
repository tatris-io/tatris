// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// binary entry point for the standalone mode
package main

import (
	"github.com/tatris-io/tatris/internal/ingestion"
	"github.com/tatris-io/tatris/internal/meta"
	"github.com/tatris-io/tatris/internal/query"
	"github.com/tatris-io/tatris/internal/service"
)

func main() {
	ingestion.SayHello()
	meta.SayHello()
	query.SayHello()
	service.SayHello()
	println("standalone")
}
