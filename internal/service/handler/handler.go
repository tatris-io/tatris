// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/protocol"
)

// OK serialize a given struct as JSON into the HTTP context and set the status code to 200
func OK(c *gin.Context, response any) {
	c.JSON(http.StatusOK, response)
}

// ACK serialize a response body carrying `Acknowledged=true` into the HTTP context and set the
// status code to 200
func ACK(c *gin.Context) {
	c.JSON(http.StatusOK, &protocol.Response{Acknowledged: true})
}

// NotFound serialize a response body carrying resource information not found into the HTTP context
// and set the status code to 404
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

// BadRequest serialize a response body carrying the reason for requesting incorrectly into the HTTP
// context and set the status code to 400
func BadRequest(c *gin.Context, reason string) {
	response := &protocol.Response{Error: &protocol.Error{Err: &protocol.Err{Reason: reason}}}
	c.JSON(http.StatusBadRequest, response)
}

// InternalServerError serialize a response body carrying the reason of server exception into the
// HTTP context and set the status code to 500
func InternalServerError(c *gin.Context, reason string) {
	response := &protocol.Response{Error: &protocol.Error{Err: &protocol.Err{Reason: reason}}}
	c.JSON(http.StatusInternalServerError, response)
}
