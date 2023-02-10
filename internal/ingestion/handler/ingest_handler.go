// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about ingestion
package handler

import (
	"errors"
	"net/http"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/ingestion"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
)

func IngestHandler(c *gin.Context) {
	name := c.Param("index")
	if _, err := metadata.GetIndex(name); err != nil {
		var notFoundErr *errs.IndexNotFoundError
		if errors.As(err, &notFoundErr) {
			c.JSON(
				http.StatusNotFound,
				protocol.Response{Code: http.StatusNotFound, Err: err},
			)
		} else {
			c.JSON(http.StatusInternalServerError, protocol.Response{Code: http.StatusInternalServerError, Err: err, Message: "index get failed"})
		}
	} else {
		ingestRequest := protocol.IngestRequest{}
		if err := c.ShouldBind(&ingestRequest); err != nil {
			c.JSON(http.StatusBadRequest, protocol.Response{Code: http.StatusBadRequest, Err: err, Message: "invalid request"})
			return
		}
		if err := ingestion.IngestDocs(name, ingestRequest.Documents); err != nil {
			c.JSON(http.StatusInternalServerError, protocol.Response{Code: http.StatusInternalServerError, Err: err, Message: "ingest failed"})
		} else {
			c.JSON(http.StatusOK, nil)
		}
	}
}
