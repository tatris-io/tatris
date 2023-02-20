// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about query
package handler

import (
	"net/http"
	"strings"

	"github.com/tatris-io/tatris/internal/meta/metadata"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/core"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/protocol"
	"github.com/tatris-io/tatris/internal/query"
)

func QueryHandler(c *gin.Context) {
	index := c.Param("index")
	names := strings.Split(strings.TrimSpace(index), consts.Comma)
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
	indexNames := make([]string, 0)
	for _, n := range names {
		indexNames = append(indexNames, metadata.ResolveIndexes(n)...)
	}
	indexes := make([]*core.Index, len(indexNames))
	for i, indexName := range indexNames {
		if exist, index := CheckIndexExistence(indexName, c); exist {
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
