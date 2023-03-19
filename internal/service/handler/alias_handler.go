// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

import (
	"fmt"
	"strings"

	"github.com/tatris-io/tatris/internal/common/utils"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
)

func ManageAliasHandler(c *gin.Context) {
	req := protocol.AliasManageRequest{}
	if err := c.ShouldBind(&req); err != nil {
		BadRequest(c, err.Error())
	} else {
		actions := req.Actions
		for _, action := range actions {
			if len(action) > 1 {
				BadRequest(c, "Too many operations declared on operation entry")
			} else {
				for name, term := range action {
					if term.Index == "" || term.Alias == "" {
						if term.Index == "" {
							BadRequest(c, "index is required")
						} else {
							BadRequest(c, "alias is required")
						}
						return
					} else if exist, _ := metadata.GetIndexExplicitly(term.Alias); exist != nil {
						BadRequest(c, fmt.Sprintf("Invalid alias name [%s]: an index or data stream exists with the same name as the alias", term.Alias))
						return
					} else {
						// TODO: check the legality of the alias name,
						// for example, it cannot contain *,?, etc.
						if strings.EqualFold(name, "add") {
							if err := metadata.AddAlias(term); err != nil {
								InternalServerError(c, err.Error())
								return
							}
						} else if strings.EqualFold(name, "remove") {
							if err := metadata.RemoveAlias(term); err != nil {
								InternalServerError(c, err.Error())
								return
							}
						} else {
							BadRequest(c, fmt.Sprintf("[alias_action] unknown field [%s]", name))
							return
						}
					}
				}
			}
		}
	}
	ACK(c)
}

func GetAliasHandler(c *gin.Context) {
	indexName := c.Param("index")
	aliasName := c.Param("alias")
	if indexName != "" && !utils.ContainsWildcard(indexName) {
		// if the index is specified explicitly, check its existence first
		if _, err := metadata.GetIndexExplicitly(indexName); err != nil {
			if ok, infErr := errs.IndexNotFound(err); ok {
				NotFound(c, "index", infErr.Index)
			} else {
				InternalServerError(c, err.Error())
			}
			return
		}
	}
	terms := metadata.GetAliasTerms(indexName, aliasName)
	OK(c, generateAliasResp(terms...))
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
