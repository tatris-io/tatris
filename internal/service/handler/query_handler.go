// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about query
package handler

import (
	"net/http"
	"strings"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/core"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/protocol"
	"github.com/tatris-io/tatris/internal/query"
)

func QueryHandler(c *gin.Context) {
	index := c.Param("index")
	names := strings.Split(index, consts.Comma)
	queryRequest := protocol.QueryRequest{Index: index, Size: 10}
	if err := c.ShouldBind(&queryRequest); err != nil || len(names) == 0 {
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

	indexes := make([]*core.Index, len(names))
	for i, name := range names {
		if exist, index := CheckIndexExistence(name, c); exist {
			indexes[i] = index
		} else {
			return
		}
	}

	resp, err := query.SearchDocs(indexes, queryRequest)
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
