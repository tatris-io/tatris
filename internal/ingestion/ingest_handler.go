// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package ingestion

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func IngestHandler(c *gin.Context) {
	indexName := c.Param("index")
	ingestRequest := IngestRequest{}
	if err := c.ShouldBind(&ingestRequest); err != nil {
		c.String(http.StatusBadRequest, `invalid request`)
	}
	ingestRequest.Index = indexName
	// TODO do ingestion...
	c.JSON(http.StatusOK, ingestRequest)
}
