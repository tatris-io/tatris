// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about query
package handler

import (
	"net/http"
	"strings"
	"time"

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
	start := time.Now()
	if err := c.ShouldBind(&queryRequest); err != nil || len(names) == 0 {
		c.JSON(
			http.StatusBadRequest,
			protocol.Response{
				Took:    time.Since(start).Milliseconds(),
				Error:   true,
				Message: err.Error(),
			},
		)
		return
	}

	// the param typedKeys is used to carry the aggregation type to the aggregation result, which is
	// usually used to help the client perform correct JSON deserialization see:
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations.html#return-agg-type
	typedKeys := c.Request.URL.Query()[consts.TypedKeysParam]
	if typedKeys != nil && typedKeys[0] == consts.TypedKeysParamValueTrue {
		queryRequest.TypedKeys = true
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
				Took:    time.Since(start).Milliseconds(),
				Error:   true,
				Message: err.Error(),
			},
		)
	} else {
		c.JSON(http.StatusOK, resp)
	}
}
