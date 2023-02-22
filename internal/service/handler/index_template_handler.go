// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is about how to handle HTTP requests for meta
package handler

import (
	"net/http"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
)

func CreateIndexTemplateHandler(c *gin.Context) {
	name := c.Param("template")
	template := &protocol.IndexTemplate{}
	if err := c.ShouldBind(template); err != nil {
		c.JSON(
			http.StatusBadRequest,
			protocol.Response{Code: http.StatusBadRequest, Err: err, Message: "invalid request"},
		)
		return
	}
	template.Name = name
	if err := metadata.CreateIndexTemplate(template); err != nil {
		c.JSON(
			http.StatusInternalServerError,
			protocol.Response{
				Code:    http.StatusInternalServerError,
				Err:     err,
				Message: "template create failed",
			},
		)
	} else {
		c.JSON(http.StatusOK, protocol.Response{Code: http.StatusOK, Data: template})
	}
}

func GetIndexTemplateHandler(c *gin.Context) {
	name := c.Param("template")
	if exist, template := CheckIndexTemplateExistence(name, c); exist {
		c.JSON(http.StatusOK, template)
	}
}

func IndexTemplateExistHandler(c *gin.Context) {
	name := c.Param("template")
	if exist, _ := CheckIndexTemplateExistence(name, c); exist {
		c.JSON(http.StatusOK, nil)
	}
}

func DeleteIndexTemplateHandler(c *gin.Context) {
	name := c.Param("template")
	if exist, template := CheckIndexTemplateExistence(name, c); exist {
		if err := metadata.DeleteIndexTemplate(name); err != nil {
			c.JSON(
				http.StatusInternalServerError,
				protocol.Response{
					Code:    http.StatusInternalServerError,
					Err:     err,
					Message: "template delete failed",
				},
			)
		} else {
			c.JSON(http.StatusOK, template)
		}
	}
}

func CheckIndexTemplateExistence(name string, c *gin.Context) (bool, *protocol.IndexTemplate) {
	if template, err := metadata.GetIndexTemplate(name); template != nil && err == nil {
		return true, template
	} else if errs.IsIndexTemplateNotFound(err) {
		c.JSON(
			http.StatusNotFound,
			protocol.Response{Code: http.StatusNotFound, Err: err},
		)
	} else {
		c.JSON(http.StatusInternalServerError, protocol.Response{Code: http.StatusInternalServerError, Err: err, Message: "template get failed"})
	}
	return false, nil
}
