// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is about how to handle HTTP requests for meta
package handler

import (
	"errors"
	"net/http"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
)

func CreateIndexHandler(c *gin.Context) {
	name := c.Param("index")
	if exist, err := metadata.GetIndex(name); err != nil && !errs.IsIndexNotFound(err) {
		c.JSON(
			http.StatusInternalServerError,
			protocol.Response{
				Code:    http.StatusInternalServerError,
				Err:     err,
				Message: "index get failed",
			},
		)
	} else if exist != nil {
		c.JSON(http.StatusBadRequest, protocol.Response{Code: http.StatusBadRequest, Message: "index already exists"})
	} else {
		index := protocol.Index{}
		if err := c.ShouldBind(&index); err != nil {
			c.JSON(http.StatusBadRequest, protocol.Response{Code: http.StatusBadRequest, Err: err, Message: "invalid request"})
			return
		}
		index.Name = name
		if err := metadata.CreateIndex(&core.Index{Index: &index}); err != nil {
			c.JSON(http.StatusInternalServerError, protocol.Response{Code: http.StatusInternalServerError, Err: err, Message: "index create failed"})
		} else {
			c.JSON(http.StatusOK, protocol.Response{Code: http.StatusOK, Data: index})
		}
	}
}

func GetIndexHandler(c *gin.Context) {
	name := c.Param("index")
	if exist, index := CheckIndexExistence(name, c); exist {
		c.JSON(http.StatusOK, index)
	}
}

func IndexExistHandler(c *gin.Context) {
	name := c.Param("index")
	if exist, _ := CheckIndexExistence(name, c); exist {
		c.JSON(http.StatusOK, nil)
	}
}

func DeleteIndexHandler(c *gin.Context) {
	name := c.Param("index")
	if exist, index := CheckIndexExistence(name, c); exist {
		if err := metadata.DeleteIndex(name); err != nil {
			c.JSON(
				http.StatusInternalServerError,
				protocol.Response{
					Code:    http.StatusInternalServerError,
					Err:     err,
					Message: "index delete failed",
				},
			)
		} else {
			c.JSON(http.StatusOK, index)
		}
	}
}

// CheckIndexExistence encapsulates common code snippets for checking index existence
// returns true if the index exists
// otherwise returns false and outputs an error message to the HTTP body
func CheckIndexExistence(name string, c *gin.Context) (bool, *core.Index) {
	var index *core.Index
	var err error
	if index, err = metadata.GetIndex(name); index != nil && err == nil {
		return true, index
	}
	var notFoundErr *errs.IndexNotFoundError
	if errors.As(err, &notFoundErr) {
		c.JSON(
			http.StatusNotFound,
			protocol.Response{Code: http.StatusNotFound, Err: err},
		)
	} else {
		c.JSON(http.StatusInternalServerError, protocol.Response{Code: http.StatusInternalServerError, Err: err, Message: "index get failed"})
	}
	return false, nil
}
