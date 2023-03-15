// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/tatris-io/tatris/internal/common/utils"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
)

func ManageAliasHandler(c *gin.Context) {
	start := time.Now()
	code := http.StatusOK
	response := protocol.Response{}
	req := protocol.AliasManageRequest{}
	if err := c.ShouldBind(&req); err != nil {
		code = http.StatusBadRequest
		response.Error = true
		response.Message = err.Error()
	} else {
		actions := req.Actions
		for _, action := range actions {
			if len(action) > 1 {
				code = http.StatusBadRequest
				response.Error = true
				response.Message = "Too many operations declared on operation entry"
			} else {
				for name, term := range action {
					if term.Index == "" || term.Alias == "" {
						code = http.StatusBadRequest
						response.Error = true
						if term.Index == "" {
							response.Message = "index is required"
						} else {
							response.Message = "alias is required"
						}
					} else if exist, _ := metadata.GetIndexPrecisely(term.Alias); exist != nil {
						code = http.StatusBadRequest
						response.Error = true
						response.Message = fmt.Sprintf("Invalid alias name [%s]: an index or data stream exists with the same name as the alias", term.Alias)
					} else {
						// TODO: check the legality of the alias name,
						// for example, it cannot contain *,?, etc.
						if strings.EqualFold(name, "add") {
							if err := metadata.AddAlias(term); err != nil {
								code = http.StatusInternalServerError
								response.Error = true
								response.Message = err.Error()
							}
						} else if strings.EqualFold(name, "remove") {
							if err := metadata.RemoveAlias(term); err != nil {
								code = http.StatusInternalServerError
								response.Error = true
								response.Message = err.Error()
							}
						} else {
							code = http.StatusBadRequest
							response.Error = true
							response.Message = fmt.Sprintf("unsupported action: %s", name)
						}
					}
				}
				if code != http.StatusOK {
					break
				}
			}
		}
	}
	response.Took = time.Since(start).Milliseconds()
	c.JSON(code, response)
}

func GetAliasHandler(c *gin.Context) {
	start := time.Now()
	indexName := c.Param("index")
	aliasName := c.Param("alias")
	var resp protocol.AliasGetResponse
	code := http.StatusOK
	response := protocol.Response{}
	if indexName != "" && !utils.ContainsWildcard(indexName) {
		// if the index is specified explicitly, check its existence first
		if _, err := metadata.GetIndexPrecisely(indexName); err != nil {
			if errs.IsIndexNotFound(err) {
				code = http.StatusNotFound
			} else {
				code = http.StatusInternalServerError
			}
			response.Error = true
			response.Message = err.Error()
			response.Took = time.Since(start).Milliseconds()
			c.JSON(code, response)
			return
		}
	}
	terms := metadata.GetAliasTerms(indexName, aliasName)
	resp = generateAliasResp(terms...)
	c.JSON(http.StatusOK, resp)
}

func generateAliasResp(aliasTerms ...*protocol.AliasTerm) protocol.AliasGetResponse {
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
