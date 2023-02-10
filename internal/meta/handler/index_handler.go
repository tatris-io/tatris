// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is about how to handle HTTP requests for meta
package handler

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
)

func CreateIndexHandler(c *gin.Context) {
	indexName := c.Param("index")
	if exist, err := metadata.GetIndex(indexName); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"msg": err.Error()})
	} else if exist != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("index already exists: %s", indexName)})
	} else {
		index := protocol.Index{}
		if err := c.ShouldBind(&index); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("invalid request: %+v", err.Error())})
			return
		}
		index.Name = indexName
		if err := metadata.CreateIndex(&core.Index{Index: &index}); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"msg": err.Error()})
		} else {
			c.JSON(http.StatusOK, index)
		}
	}
}

func GetIndexHandler(c *gin.Context) {
	indexName := c.Param("index")
	if index, err := metadata.GetIndex(indexName); err != nil {
		c.JSON(
			http.StatusInternalServerError,
			gin.H{"msg": "get index fail: " + indexName + ", " + err.Error()},
		)
	} else if index == nil {
		c.JSON(http.StatusNotFound, gin.H{"msg": fmt.Sprintf("index not found: %s", indexName)})
	} else {
		c.JSON(http.StatusOK, index)
	}
}

func IndexExistHandler(c *gin.Context) {
	indexName := c.Param("index")
	if index, err := metadata.GetIndex(indexName); err != nil {
		c.JSON(
			http.StatusInternalServerError,
			gin.H{"msg": "get index fail: " + indexName + ", " + err.Error()},
		)
	} else if index == nil {
		c.JSON(http.StatusNotFound, gin.H{"msg": fmt.Sprintf("index not found: %s", indexName)})
	} else {
		c.JSON(http.StatusOK, nil)
	}
}

func DeleteIndexHandler(c *gin.Context) {
	indexName := c.Param("index")
	if err := metadata.DeleteIndex(indexName); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"msg": err.Error()})
	} else {
		c.JSON(http.StatusOK, nil)
	}
}
