// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is about how to handle HTTP requests for query
package handler

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func QueryHandler(c *gin.Context) {
	indexName := c.Param("index")
	queryRequest := QueryRequest{Size: 10}
	if err := c.ShouldBind(&queryRequest); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": "invalid request"})
	}
	queryRequest.Index = indexName
	// TODO do search...
	c.JSON(http.StatusOK, queryRequest)
}
