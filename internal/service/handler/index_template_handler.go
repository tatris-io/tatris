// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is about how to handle HTTP requests for meta
package handler

import (
	"net/http"
	"time"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
)

func CreateIndexTemplateHandler(c *gin.Context) {
	start := time.Now()
	name := c.Param("template")
	template := &protocol.IndexTemplate{}
	code := http.StatusOK
	response := protocol.Response{}
	if err := c.ShouldBind(template); err != nil {
		code = http.StatusBadRequest
		response.Error = true
		response.Message = err.Error()
	} else {
		template.Name = name
		if err := metadata.CreateIndexTemplate(template); err != nil {
			code = http.StatusInternalServerError
			response.Error = true
			response.Message = err.Error()
		}
	}
	response.Took = time.Since(start).Milliseconds()
	c.JSON(code, response)
}

func GetIndexTemplateHandler(c *gin.Context) {
	start := time.Now()
	name := c.Param("template")
	code := http.StatusOK
	response := protocol.Response{}
	if templates, err := metadata.ResolveIndexTemplates(name); err != nil {
		if errs.IsIndexTemplateNotFound(err) {
			code = http.StatusNotFound
		} else {
			code = http.StatusInternalServerError
		}
		response.Error = true
		response.Message = err.Error()
		response.Took = time.Since(start).Milliseconds()
		c.JSON(code, response)
	} else {
		terms := make([]*protocol.IndexTemplateTerm, len(templates))
		for i, template := range templates {
			terms[i] = &protocol.IndexTemplateTerm{Name: template.Name, IndexTemplate: template}
		}
		c.JSON(http.StatusOK, protocol.IndexTemplateResponse{
			IndexTemplates: terms,
		})
	}
}

func IndexTemplateExistHandler(c *gin.Context) {
	start := time.Now()
	name := c.Param("template")
	code := http.StatusOK
	response := protocol.Response{}
	if _, err := metadata.ResolveIndexTemplates(name); err != nil {
		if errs.IsIndexTemplateNotFound(err) {
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

func DeleteIndexTemplateHandler(c *gin.Context) {
	start := time.Now()
	name := c.Param("template")
	code := http.StatusOK
	response := protocol.Response{}
	if templates, err := metadata.ResolveIndexTemplates(name); err != nil {
		if errs.IsIndexTemplateNotFound(err) {
			code = http.StatusNotFound
		} else {
			code = http.StatusInternalServerError
		}
		response.Error = true
		response.Message = err.Error()
		c.JSON(code, response)
	} else {
		for _, template := range templates {
			if err := metadata.DeleteIndexTemplate(template.Name); err != nil {
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
