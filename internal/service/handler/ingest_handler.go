// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about ingestion
package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/ingestion"
	"github.com/tatris-io/tatris/internal/protocol"
)

func IngestHandler(c *gin.Context) {
	name := c.Param("index")
	if exist, index := CheckIndexExistence(name, c); exist {
		ingestRequest := protocol.IngestRequest{}
		if err := c.ShouldBind(&ingestRequest); err != nil {
			c.JSON(
				http.StatusBadRequest,
				protocol.Response{
					Code:    http.StatusBadRequest,
					Err:     err,
					Message: "invalid request",
				},
			)
			return
		}
		if err := ingestion.IngestDocs(index, ingestRequest.Documents); err != nil {
			c.JSON(
				http.StatusInternalServerError,
				protocol.Response{
					Code:    http.StatusInternalServerError,
					Err:     err,
					Message: "ingest failed",
				},
			)
		} else {
			c.JSON(http.StatusOK, nil)
		}
	}
}
