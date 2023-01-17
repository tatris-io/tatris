// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about query
package handler

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/protocol"
	"github.com/tatris-io/tatris/internal/query"
)

func QueryHandler(c *gin.Context) {
	start := time.Now()
	indexName := c.Param("index")
	queryRequest := protocol.QueryRequest{Size: 10}
	if err := c.ShouldBind(&queryRequest); err != nil {
		c.String(http.StatusBadRequest, `invalid request`)
	}
	queryRequest.Index = indexName
	hits, err := query.SearchDocs(queryRequest)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"msg": err.Error()})
	} else {
		resp := protocol.QueryResponse{}
		resp.Hits = *hits
		resp.Took = time.Since(start).Milliseconds()
		c.JSON(http.StatusOK, resp)
	}
}
