// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/protocol"
)

func OK(c *gin.Context, response any) {
	c.JSON(http.StatusOK, response)
}

func Ack(c *gin.Context) {
	c.JSON(http.StatusOK, &protocol.Response{Acknowledged: true})
}

func NotFound(c *gin.Context, resource, ID string) {
	if resource == "" && ID == "" {
		c.JSON(http.StatusNotFound, nil)
	} else {
		response := &protocol.Response{
			Error: &protocol.Error{
				Err: &protocol.Err{
					Type:         fmt.Sprintf("%s_not_found_exception", resource),
					Reason:       fmt.Sprintf("no such %s [%s]", resource, ID),
					ResourceType: resource,
					ResourceID:   ID,
				},
			},
		}
		c.JSON(http.StatusNotFound, response)
	}
}

func BadRequest(c *gin.Context, reason string) {
	response := &protocol.Response{Error: &protocol.Error{Err: &protocol.Err{Reason: reason}}}
	c.JSON(http.StatusBadRequest, response)
}

func InternalServerError(c *gin.Context, reason string) {
	response := &protocol.Response{Error: &protocol.Error{Err: &protocol.Err{Reason: reason}}}
	c.JSON(http.StatusInternalServerError, response)
}
