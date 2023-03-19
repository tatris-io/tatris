// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about ingestion
package handler

import (
	"time"

	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/meta/metadata"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/ingestion"
	"github.com/tatris-io/tatris/internal/protocol"
)

func IngestHandler(c *gin.Context) {
	start := time.Now()
	name := c.Param("index")
	var index *core.Index
	var err error
	if index, err = metadata.GetIndexExplicitly(name); err != nil {
		if errs.IsIndexNotFound(err) {
			// create the index if it does not exist
			index = &core.Index{Index: &protocol.Index{Name: name}}
			err = metadata.CreateIndex(index)
		}
	}
	if err != nil {
		InternalServerError(c, err.Error())
	} else {
		ingestRequest := protocol.IngestRequest{}
		if err = c.ShouldBind(&ingestRequest); err != nil {
			BadRequest(c, err.Error())
		} else if err = ingestion.IngestDocs(index, ingestRequest.Documents); err != nil {
			InternalServerError(c, err.Error())
		} else {
			OK(c, protocol.IngestResponse{Took: time.Since(start).Milliseconds(), Error: false})
		}
	}
}
