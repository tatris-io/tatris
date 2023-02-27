// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
)

func ManageAliasHandler(c *gin.Context) {
	start := time.Now()
	req := protocol.AliasManageRequest{}
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(
			http.StatusBadRequest,
			protocol.Response{
				Took:    time.Since(start).Milliseconds(),
				Error:   true,
				Message: err.Error(),
			},
		)
		return
	}
	actions := req.Actions
	for _, action := range actions {
		for name, term := range action {
			if !handleAliasTerm(c, start, name, term) {
				return
			}
		}
	}
	c.JSON(http.StatusOK, actions)
}

func GetAliasHandler(c *gin.Context) {
	start := time.Now()
	indexName := c.Param("index")
	aliasName := c.Param("alias")
	var resp protocol.AliasGetResponse
	var terms []*protocol.AliasTerm
	if indexName == "" && aliasName == "" {
		// get all aliases terms
		terms = metadata.ListTerms()
	} else if indexName == "" {
		// get terms by alias
		terms = metadata.GetTermsByAlias(aliasName)
	} else {
		// by index, check index existence first
		if exist, _ := CheckIndexExistence(indexName, c); !exist {
			return
		}
		if aliasName == "" {
			// get terms by index
			terms = metadata.GetTermsByIndex(indexName)
		} else {
			// exactly get term by index and alias
			term := metadata.GetTerm(indexName, aliasName)
			if term != nil {
				terms = append(terms, term)
			}
			if len(terms) == 0 {
				c.JSON(
					http.StatusNotFound,

					protocol.Response{
						Took:    time.Since(start).Milliseconds(),
						Error:   true,
						Message: fmt.Sprintf("alias [%s] missing", aliasName),
					})
				return
			}
		}
	}
	resp = aliasResponse(terms...)
	c.JSON(http.StatusOK, resp)
}

func handleAliasTerm(c *gin.Context, start time.Time, action string, term *protocol.AliasTerm) bool {
	if term.Index == "" || term.Alias == "" {
		var msg string
		if term.Index == "" {
			msg = "One of [index] or [indices] is required"
		} else {
			msg = "One of [alias] or [aliases] is required"
		}
		c.JSON(
			http.StatusBadRequest,
			protocol.Response{
				Took:    time.Since(start).Milliseconds(),
				Error:   true,
				Message: msg,
			},
		)
		return false
	}
	if exist, _ := CheckIndexExistence(term.Index, c); !exist {
		return false
	}
	if exist, err := metadata.GetIndex(term.Alias); err != nil && !errs.IsIndexNotFound(err) {
		c.JSON(
			http.StatusInternalServerError,
			protocol.Response{
				Took:    time.Since(start).Milliseconds(),
				Error:   true,
				Message: err.Error(),
			},
		)
		return false
	} else if exist != nil {
		c.JSON(
			http.StatusBadRequest,
			protocol.Response{
				Took:    time.Since(start).Milliseconds(),
				Error:   true,
				Message: fmt.Sprintf("Invalid alias name [%s]: an index or data stream exists with the same name as the alias", term.Alias),
			},
		)
		return false
	} else {
		var err error
		code := http.StatusInternalServerError
		switch action {
		case "add":
			err = metadata.AddAlias(term)
		case "remove":
			err = metadata.RemoveAlias(term)
		default:
			err = &errs.UnsupportedError{Desc: "alias action", Value: action}
			code = http.StatusBadRequest
		}
		if err != nil {
			c.JSON(
				code,
				protocol.Response{
					Took:    time.Since(start).Milliseconds(),
					Error:   true,
					Message: err.Error(),
				},
			)
			return false
		}
	}
	return true
}

func aliasResponse(aliasTerms ...*protocol.AliasTerm) protocol.AliasGetResponse {
	resp := make(map[string]*protocol.Aliases)
	for _, term := range aliasTerms {
		if _, found := resp[term.Index]; !found {
			resp[term.Index] = &protocol.Aliases{Aliases: make(map[string]*protocol.AliasTerm)}
		}
		aliases := resp[term.Index].Aliases
		if _, found := aliases[term.Alias]; !found {
			aliases[term.Alias] = term
		}
	}
	return resp
}
