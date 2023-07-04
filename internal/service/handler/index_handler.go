// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is about how to handle HTTP requests for meta
package handler

import (
	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
)

func CreateIndexHandler(c *gin.Context) {
	name := c.Param("index")
	if exist, err := metadata.GetIndexExplicitly(name); err != nil && !errs.IsIndexNotFound(err) {
		InternalServerError(c, err.Error())
	} else if exist != nil {
		BadRequest(c, "index already exists")
	} else {
		index := protocol.Index{Name: name}
		if err := c.ShouldBind(&index); err != nil {
			BadRequest(c, err.Error())
		} else if err := metadata.CreateIndex(&core.Index{Index: &index}); err != nil {
			if errs.IsInvalidResourceNameError(err) {
				BadRequest(c, err.Error())
			} else {
				InternalServerError(c, err.Error())
			}
		} else {
			OK(c, protocol.CreateIndexResponse{
				Response:           &protocol.Response{Acknowledged: true},
				ShardsAcknowledged: true,
				Index:              name,
			})
		}
	}
}

func GetIndexHandler(c *gin.Context) {
	name := c.Param("index")
	if indexes, err := metadata.ResolveIndexes(name); err != nil {
		if ok, infErr := errs.IndexNotFound(err); ok {
			NotFound(c, "index", infErr.Index)
		} else {
			InternalServerError(c, err.Error())
		}
	} else {
		indexMap := make(map[string]*core.Index)
		for _, index := range indexes {
			indexMap[index.Name] = index
		}
		OK(c, indexMap)
	}
}

func IndexExistHandler(c *gin.Context) {
	name := c.Param("index")
	if _, err := metadata.ResolveIndexes(name); err != nil {
		if errs.IsIndexNotFound(err) {
			NotFound(c, "", "")
		} else {
			InternalServerError(c, err.Error())
		}
	} else {
		OK(c, nil)
	}
}

func DeleteIndexHandler(c *gin.Context) {
	name := c.Param("index")
	var indexes []*core.Index
	var err error
	if indexes, err = metadata.ResolveIndexes(name); err != nil {
		if ok, infErr := errs.IndexNotFound(err); ok {
			NotFound(c, "index", infErr.Index)
			return
		}
		InternalServerError(c, err.Error())
		return
	}
	for _, index := range indexes {
		if err := metadata.DeleteIndex(index.Name); err != nil {
			InternalServerError(c, err.Error())
			return
		}
	}
	ACK(c)
}
