// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about query
package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/protocol"
	"github.com/tatris-io/tatris/internal/query"
)

func QueryHandler(c *gin.Context) {
	name := c.Param("index")
	if exist, index := CheckIndexExistence(name, c); exist {
		queryRequest := protocol.QueryRequest{Index: name, Size: 10}
		if err := c.ShouldBind(&queryRequest); err != nil {
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

		resp, err := query.SearchDocs(index, queryRequest)
		if err != nil {
			c.JSON(
				http.StatusInternalServerError,
				protocol.Response{
					Code:    http.StatusInternalServerError,
					Err:     err,
					Message: "query failed",
				},
			)
		} else {
			c.JSON(http.StatusOK, resp)
		}
	}
}
