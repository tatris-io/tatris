// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about ingestion
package handler

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/ingestion"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
	"net/http"
)

func IngestHandler(c *gin.Context) {
	indexName := c.Param("index")
	if index, err := metadata.GetIndex(indexName); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"msg": "get index fail: " + indexName + ", " + err.Error()})
	} else if index == nil {
		c.JSON(http.StatusNotFound, gin.H{"msg": fmt.Sprintf("index not found: %s", indexName)})
	} else {
		ingestRequest := protocol.IngestRequest{}
		if err := c.ShouldBind(&ingestRequest); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"msg": err.Error()})
		}
		if err := ingestion.IngestDocs(indexName, ingestRequest.Documents); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"msg": err.Error()})
		} else {
			c.JSON(http.StatusOK, nil)
		}
	}
}
