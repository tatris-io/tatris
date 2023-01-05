// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about query
package handler

import (
	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/protocol"
	"github.com/tatris-io/tatris/internal/query"
	"net/http"
)

func QueryHandler(c *gin.Context) {
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
		result := protocol.QueryResponse{}
		result.Took = 0
		result.Hits = *hits
		c.JSON(http.StatusOK, result)
	}

}
