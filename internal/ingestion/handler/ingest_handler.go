// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is about how to handle HTTP requests for ingestion
package handler

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func IngestHandler(c *gin.Context) {
	indexName := c.Param("index")
	ingestRequest := IngestRequest{}
	if err := c.ShouldBind(&ingestRequest); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": "invalid request"})
	}
	ingestRequest.Index = indexName
	// TODO do ingestion...
	c.JSON(http.StatusOK, ingestRequest)
}
