// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about ingestion
package handler

import (
	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/ingestion"
	"github.com/tatris-io/tatris/internal/protocol"
	"net/http"
)

func IngestHandler(c *gin.Context) {
	indexName := c.Param("index")
	ingestRequest := protocol.IngestRequest{}
	if err := c.ShouldBind(&ingestRequest); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"msg": err.Error()})
	}
	ingestRequest.Index = indexName
	if err := ingestion.IngestDocs(indexName, ingestRequest.Documents); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"msg": err.Error()})
	} else {
		c.JSON(http.StatusOK, nil)
	}
}
