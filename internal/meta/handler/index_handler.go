// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is about how to handle HTTP requests for meta
package handler

import (
	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/meta"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"net/http"
)

func CreateIndexHandler(c *gin.Context) {
	idxName := c.Param("index")
	idx := meta.Index{}
	if err := c.ShouldBind(&idx); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": "invalid request"})
	}
	idx.Name = idxName
	if err := metadata.Create(&idx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"msg": err.Error()})
	} else {
		c.JSON(http.StatusOK, idx)
	}
}

func GetIndexHandler(c *gin.Context) {
	idxName := c.Param("index")
	if idx, err := metadata.Get(idxName); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"msg": "get index fail: " + idxName + ", " + err.Error()})
	} else if idx == nil {
		c.JSON(http.StatusNotFound, gin.H{"msg": "index not found: " + idxName})
	} else {
		c.JSON(http.StatusOK, idx)
	}
}
