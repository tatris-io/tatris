// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is about how to handle HTTP requests for meta
package handler

import (
	"net/http"
	"time"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
)

func CreateIndexHandler(c *gin.Context) {
	start := time.Now()
	name := c.Param("index")
	code := http.StatusOK
	response := protocol.Response{}
	if exist, err := metadata.GetIndexExplicitly(name); err != nil && !errs.IsIndexNotFound(err) {
		code = http.StatusInternalServerError
		response.Error = true
		response.Message = err.Error()
	} else if exist != nil {
		code = http.StatusBadRequest
		response.Error = true
		response.Message = "index already exists"
	} else {
		index := protocol.Index{Name: name}
		if err := c.ShouldBind(&index); err != nil {
			code = http.StatusBadRequest
			response.Error = true
			response.Message = err.Error()
		} else if err := metadata.CreateIndex(&core.Index{Index: &index}); err != nil {
			code = http.StatusInternalServerError
			response.Error = true
			response.Message = err.Error()
		}
	}
	response.Took = time.Since(start).Milliseconds()
	c.JSON(code, response)
}

func GetIndexHandler(c *gin.Context) {
	start := time.Now()
	name := c.Param("index")
	code := http.StatusOK
	response := protocol.Response{}
	if indexes, err := metadata.ResolveIndexes(name); err != nil {
		if errs.IsIndexNotFound(err) {
			code = http.StatusNotFound
		} else {
			code = http.StatusInternalServerError
		}
		response.Error = true
		response.Message = err.Error()
		response.Took = time.Since(start).Milliseconds()
		c.JSON(code, response)
	} else {
		indexMap := make(map[string]*core.Index)
		for _, index := range indexes {
			indexMap[index.Name] = index
		}
		c.JSON(code, indexMap)
	}
}

func IndexExistHandler(c *gin.Context) {
	start := time.Now()
	name := c.Param("index")
	code := http.StatusOK
	response := protocol.Response{}
	if _, err := metadata.ResolveIndexes(name); err != nil {
		if errs.IsIndexNotFound(err) {
			code = http.StatusNotFound
		} else {
			code = http.StatusInternalServerError
		}
		response.Error = true
		response.Message = err.Error()
	}
	response.Took = time.Since(start).Milliseconds()
	c.JSON(code, response)
}

func DeleteIndexHandler(c *gin.Context) {
	start := time.Now()
	name := c.Param("index")
	code := http.StatusOK
	response := protocol.Response{}
	if indexes, err := metadata.ResolveIndexes(name); err != nil {
		if errs.IsIndexNotFound(err) {
			code = http.StatusNotFound
		} else {
			code = http.StatusInternalServerError
		}
		response.Error = true
		response.Message = err.Error()
		c.JSON(code, response)
	} else {
		for _, index := range indexes {
			if err := metadata.DeleteIndex(index.Name); err != nil {
				code = http.StatusInternalServerError
				response.Error = true
				response.Message = err.Error()
				break
			}
		}
	}
	response.Took = time.Since(start).Milliseconds()
	c.JSON(code, response)

}
