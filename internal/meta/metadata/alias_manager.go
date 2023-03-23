// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package metadata is about the management of metadata (i.e. index)
package metadata

import (
	"encoding/json"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/bobg/go-generics/set"
	"github.com/tatris-io/tatris/internal/common/utils"

	cache "github.com/patrickmn/go-cache"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/protocol"
	"go.uber.org/zap"
)

// ResolveAliases tries to resolve index names by alias expressions
func ResolveAliases(name string) []string {
	// use set to deduplicate
	indexes := set.Of[string]{}
	terms := GetAliasTerms("", name)
	for _, t := range terms {
		indexes.Add(t.Index)
	}
	return indexes.Slice()
}

func AddAlias(aliasTerm *protocol.AliasTerm) error {

	index := aliasTerm.Index
	alias := aliasTerm.Alias

	if err := utils.ValidateResourceName(alias); err != nil {
		return err
	}

	if existIndex, _ := GetIndexExplicitly(alias); existIndex != nil {
		return &errs.InvalidResourceNameError{
			Name:    alias,
			Message: "an index or data stream exists with the same name as the alias",
		}
	}

	logger.Info(
		"add alias",
		zap.String("alias", alias),
		zap.String("index", index),
	)

	Instance().AliasTermsCache.Set(aliasTermKey(index, alias), aliasTerm, cache.NoExpiration)

	indexTermsJSON, err := json.Marshal(aliasTerm)
	if err != nil {
		return err
	}
	return Instance().MStore.Set(aliasPrefix(aliasTermKey(index, alias)), indexTermsJSON)
}

// RemoveAlias supports removing alias terms in the form of wildcards
func RemoveAlias(aliasTerm *protocol.AliasTerm) error {

	index := aliasTerm.Index
	alias := aliasTerm.Alias
	terms := GetAliasTerms(index, alias)

	logger.Info(
		"remove alias",
		zap.String("alias", alias),
		zap.String("index", index),
		zap.Any("terms", terms),
	)

	for _, term := range terms {
		Instance().AliasTermsCache.Delete(aliasTermKey(term.Index, term.Alias))
		if err := Instance().MStore.Delete(aliasPrefix(aliasTermKey(term.Index, term.Alias))); err != nil {
			return err
		}
	}
	return nil
}

func RemoveAliasesByIndex(index string) error {
	terms := GetAliasTerms(index, "")
	for _, term := range terms {
		if err := RemoveAlias(term); err != nil {
			return err
		}
	}
	return nil
}

func GetAliasTerms(index, alias string) []*protocol.AliasTerm {

	var terms []*protocol.AliasTerm

	for _, item := range Instance().AliasTermsCache.Items() {
		term := item.Object.(*protocol.AliasTerm)
		if (index == "" || utils.WildcardMatch(index, term.Index)) &&
			(alias == "" || utils.WildcardMatch(alias, term.Alias)) {
			terms = append(terms, term)
		}
	}
	return terms
}

func aliasPrefix(name string) string {
	return AliasPath + name
}
