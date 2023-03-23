// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package handler is about how to handle HTTP requests for meta
package handler

import (
	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
)

func CreateIndexTemplateHandler(c *gin.Context) {
	name := c.Param("template")
	template := &protocol.IndexTemplate{Name: name}
	if err := c.ShouldBind(template); err != nil {
		BadRequest(c, err.Error())
	} else if err := metadata.CreateIndexTemplate(template); err != nil {
		InternalServerError(c, err.Error())
	} else {
		ACK(c)
	}
}

func GetIndexTemplateHandler(c *gin.Context) {
	name := c.Param("template")
	if templates, err := metadata.ResolveIndexTemplates(name); err != nil {
		if ok, itnfErr := errs.IndexTemplateNotFound(err); ok {
			NotFound(c, "index_template", itnfErr.IndexTemplate)
		} else {
			InternalServerError(c, err.Error())
		}
	} else {
		terms := make([]*protocol.IndexTemplateTerm, len(templates))
		for i, template := range templates {
			terms[i] = &protocol.IndexTemplateTerm{Name: template.Name, IndexTemplate: template}
		}
		OK(c, protocol.IndexTemplateResponse{
			IndexTemplates: terms,
		})
	}
}

func IndexTemplateExistHandler(c *gin.Context) {
	name := c.Param("template")
	if _, err := metadata.ResolveIndexTemplates(name); err != nil {
		if errs.IsIndexTemplateNotFound(err) {
			NotFound(c, "", "")
		} else {
			InternalServerError(c, err.Error())
		}
	} else {
		OK(c, nil)
	}
}

func DeleteIndexTemplateHandler(c *gin.Context) {
	name := c.Param("template")
	if templates, err := metadata.ResolveIndexTemplates(name); err != nil {
		if ok, itnfErr := errs.IndexTemplateNotFound(err); ok {
			NotFound(c, "index_template", itnfErr.IndexTemplate)
		} else {
			InternalServerError(c, err.Error())
		}
	} else {
		for _, template := range templates {
			if err := metadata.DeleteIndexTemplate(template.Name); err != nil {
				InternalServerError(c, err.Error())
				return
			}
		}
		ACK(c)
	}
}
