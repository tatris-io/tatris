// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package meta

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

func CreateIndexHandler(c *gin.Context) {
	indexName := c.Param("index")
	index := Index{}
	if err := c.ShouldBind(&index); err != nil {
		c.String(http.StatusBadRequest, `invalid request`)
	}
	index.Name = indexName
	// TODO do index creation...
	c.JSON(http.StatusOK, index)
}
