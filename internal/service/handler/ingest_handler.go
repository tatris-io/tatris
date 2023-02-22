// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about ingestion
package handler

import (
	"net/http"

	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/meta/metadata"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/ingestion"
	"github.com/tatris-io/tatris/internal/protocol"
)

func IngestHandler(c *gin.Context) {
	name := c.Param("index")
	var index *core.Index
	var err error
	if index, err = metadata.GetIndex(name); err != nil {
		if errs.IsIndexNotFound(err) {
			// create the index if it does not exist
			index = &core.Index{Index: &protocol.Index{Name: name}}
			err = metadata.CreateIndex(index)
		}
		if err != nil {
			c.JSON(
				http.StatusInternalServerError,
				protocol.Response{Code: http.StatusInternalServerError, Err: err},
			)
			return
		}
	}
	ingestRequest := protocol.IngestRequest{}
	if err := c.ShouldBind(&ingestRequest); err != nil {
		c.JSON(
			http.StatusBadRequest,
			protocol.Response{
				Code: http.StatusBadRequest,
				Err:  err,
			},
		)
		return
	}
	if err := ingestion.IngestDocs(index, ingestRequest.Documents); err != nil {
		c.JSON(
			http.StatusInternalServerError,
			protocol.Response{
				Code: http.StatusInternalServerError,
				Err:  err,
			},
		)
	} else {
		c.JSON(http.StatusOK, nil)
	}
}
