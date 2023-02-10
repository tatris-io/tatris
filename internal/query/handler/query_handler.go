// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is responsible for handling HTTP requests about query
package handler

import (
	"errors"
	"net/http"

	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/meta/metadata"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/protocol"
	"github.com/tatris-io/tatris/internal/query"
)

func QueryHandler(c *gin.Context) {
	name := c.Param("index")
	if _, err := metadata.GetIndex(name); err != nil {
		var notFoundErr *errs.IndexNotFoundError
		if errors.As(err, &notFoundErr) {
			c.JSON(http.StatusNotFound, protocol.Response{Code: http.StatusNotFound, Err: err})
		} else {
			c.JSON(http.StatusInternalServerError, protocol.Response{Code: http.StatusInternalServerError, Err: err, Message: "index get failed"})
		}
	} else {
		queryRequest := protocol.QueryRequest{Index: name, Size: 10}
		if err := c.ShouldBind(&queryRequest); err != nil {
			c.JSON(http.StatusBadRequest, protocol.Response{Code: http.StatusBadRequest, Err: err, Message: "invalid request"})
			return
		}

		resp, err := query.SearchDocs(queryRequest)
		if err != nil {
			c.JSON(http.StatusInternalServerError, protocol.Response{Code: http.StatusInternalServerError, Err: err, Message: "query failed"})
		} else {
			c.JSON(http.StatusOK, resp)
		}
	}
}
