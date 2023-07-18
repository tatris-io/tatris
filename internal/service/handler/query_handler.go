// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about query
package handler

import (
	"strings"
	"time"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/tatris-io/tatris/internal/meta/metadata"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/protocol"
	"github.com/tatris-io/tatris/internal/query"
)

func QueryHandler(c *gin.Context) {
	start := time.Now()
	index := c.Param("index")
	names := strings.Split(strings.TrimSpace(index), consts.Comma)
	queryRequest := protocol.QueryRequest{Index: index, Size: 10}
	if err := c.ShouldBindJSON(&queryRequest); err != nil || len(names) == 0 {
		BadRequest(c, err.Error())
	} else if indexes, err := metadata.ResolveIndexes(index); err != nil {
		if ok, infErr := errs.IndexNotFound(err); ok {
			NotFound(c, "index", infErr.Index)
		} else {
			InternalServerError(c, err.Error())
		}
	} else {
		// the param typedKeys is used to carry the aggregation type to the aggregation result,
		// which is
		// usually used to help the client perform correct JSON deserialization see:
		// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations.html#return-agg-type
		typedKeys := c.Request.URL.Query()[consts.TypedKeysParam]
		if typedKeys != nil && typedKeys[0] == consts.TypedKeysParamValueTrue {
			queryRequest.TypedKeys = true
		}
		if queryResponse, err := query.SearchDocs(indexes, queryRequest); err != nil {
			InternalServerError(c, err.Error())
		} else {
			queryResponse.Took = time.Since(start).Milliseconds()
			OK(c, queryResponse)
		}
	}
}
