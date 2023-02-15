// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package metadata is about the management of metadata (i.e. index)
package metadata

import (
	"encoding/json"

	"github.com/patrickmn/go-cache"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/protocol"
	"go.uber.org/zap"
)

func AddAlias(aliasTerm *protocol.AliasTerm) error {

	index := aliasTerm.Index
	alias := aliasTerm.Alias

	aliasTerms := GetTermsByAlias(alias)
	aliasTerms = add(aliasTerms, aliasTerm)

	indexTerms := GetTermsByIndex(index)
	indexTerms = add(indexTerms, aliasTerm)

	logger.Info(
		"add alias",
		zap.String("alias", alias),
		zap.String("index", index),
		zap.Any("aliasTerms", aliasTerms),
		zap.Any("indexTerms", indexTerms),
	)
	return SaveAlias(alias, aliasTerms, index, indexTerms)
}

func RemoveAlias(aliasTerm *protocol.AliasTerm) error {

	index := aliasTerm.Index
	alias := aliasTerm.Alias

	aliasTerms := GetTermsByAlias(alias)
	aliasTerms = remove(aliasTerms, aliasTerm)

	indexTerms := GetTermsByIndex(index)
	indexTerms = remove(indexTerms, aliasTerm)

	logger.Info(
		"remove alias",
		zap.String("alias", alias),
		zap.String("index", index),
		zap.Any("aliasTerms", aliasTerms),
		zap.Any("indexTerms", indexTerms),
	)
	return SaveAlias(alias, aliasTerms, index, indexTerms)
}

func add(terms []*protocol.AliasTerm, term *protocol.AliasTerm) []*protocol.AliasTerm {
	newTerms := remove(terms, term)
	newTerms = append(newTerms, term)
	return newTerms
}

func remove(terms []*protocol.AliasTerm, term *protocol.AliasTerm) []*protocol.AliasTerm {
	i := 0
	for _, t := range terms {
		if t.Index != term.Index || t.Alias != term.Alias {
			terms[i] = t
			i++
		}
	}
	return terms[:i]
}

func ListAllAliases() []*protocol.AliasTerm {
	items := AliasTermsCache.Items()
	terms := make([]*protocol.AliasTerm, 0)
	for _, item := range items {
		terms = append(terms, item.Object.([]*protocol.AliasTerm)...)
	}
	return terms
}

func GetTermsByAlias(alias string) []*protocol.AliasTerm {
	var terms []*protocol.AliasTerm
	cached, found := AliasTermsCache.Get(alias)
	if found {
		terms = cached.([]*protocol.AliasTerm)
	}
	return terms
}

func GetTermsByIndex(index string) []*protocol.AliasTerm {
	terms := make([]*protocol.AliasTerm, 0)
	cached, found := IndexTermsCache.Get(index)
	if found {
		terms = cached.([]*protocol.AliasTerm)
	}
	return terms
}

func GetTerm(index, alias string) *protocol.AliasTerm {
	indexTerms := GetTermsByIndex(index)
	aliasTerms := GetTermsByAlias(alias)
	for _, t1 := range indexTerms {
		for _, t2 := range aliasTerms {
			if t1.Index == t2.Index && t1.Alias == t2.Alias {
				return t1
			}
		}
	}
	return nil
}

func SaveAlias(
	alias string,
	aliasTerms []*protocol.AliasTerm,
	index string,
	indexTerms []*protocol.AliasTerm,
) error {
	AliasTermsCache.Set(alias, aliasTerms, cache.NoExpiration)
	IndexTermsCache.Set(index, indexTerms, cache.NoExpiration)

	indexTermsJSON, err := json.Marshal(indexTerms)
	if err != nil {
		return err
	}
	return MStore.Set(aliasPrefix(index), indexTermsJSON)
}

func aliasPrefix(name string) string {
	return AliasPath + name
}
