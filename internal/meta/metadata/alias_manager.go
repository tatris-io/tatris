// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package metadata is about the management of metadata (i.e. index)
package metadata

import (
	"encoding/json"

	"github.com/bobg/go-generics/slices"

	"github.com/patrickmn/go-cache"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/protocol"
	"go.uber.org/zap"
)

var (
	// aliasTermsCache caches { alias -> []AliasTerm }
	aliasTermsCache = cache.New(
		cache.NoExpiration,
		cache.NoExpiration,
	)
	// indexTermsCache caches { index -> []AliasTerm }
	indexTermsCache = cache.New(
		cache.NoExpiration,
		cache.NoExpiration,
	)
)

func LoadAliases() error {
	bytesMap, err := MStore.List(AliasPath)
	if err != nil {
		return err
	}
	terms := make([]*protocol.AliasTerm, 0)
	for _, bytes := range bytesMap {
		ts := make([]*protocol.AliasTerm, 0)
		if err := json.Unmarshal(bytes, &ts); err != nil {
			return err
		}
		terms = append(terms, ts...)
	}
	groupByAlias, err := slices.Group(terms, func(t *protocol.AliasTerm) (string, error) {
		return t.Alias, nil
	})
	if err != nil {
		return err
	}
	for alias, terms := range groupByAlias {
		aliasTermsCache.Set(alias, terms, cache.NoExpiration)
	}
	groupByIndex, err := slices.Group(terms, func(t *protocol.AliasTerm) (string, error) {
		return t.Index, nil
	})
	if err != nil {
		return err
	}
	for index, terms := range groupByIndex {
		indexTermsCache.Set(index, terms, cache.NoExpiration)
	}
	return nil
}

func ResolveIndexes(name string) []string {
	terms := GetTermsByAlias(name)
	if len(terms) == 0 {
		return []string{name}
	}
	indexes := make([]string, len(terms))
	for i, t := range terms {
		indexes[i] = t.Index
	}
	return indexes
}

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
	return saveAlias(alias, aliasTerms, index, indexTerms)
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
	return saveAlias(alias, aliasTerms, index, indexTerms)
}

func RemoveAliasesByIndex(index string) error {
	terms := GetTermsByIndex(index)
	for _, term := range terms {
		if err := RemoveAlias(term); err != nil {
			return err
		}
	}
	return nil
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

func ListTerms() []*protocol.AliasTerm {
	items := aliasTermsCache.Items()
	terms := make([]*protocol.AliasTerm, 0)
	for _, item := range items {
		terms = append(terms, item.Object.([]*protocol.AliasTerm)...)
	}
	return terms
}

func GetTermsByAlias(alias string) []*protocol.AliasTerm {
	var terms []*protocol.AliasTerm
	cached, found := aliasTermsCache.Get(alias)
	if found {
		terms = cached.([]*protocol.AliasTerm)
	}
	return terms
}

func GetTermsByIndex(index string) []*protocol.AliasTerm {
	terms := make([]*protocol.AliasTerm, 0)
	cached, found := indexTermsCache.Get(index)
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

func saveAlias(
	alias string,
	aliasTerms []*protocol.AliasTerm,
	index string,
	indexTerms []*protocol.AliasTerm,
) error {
	aliasTermsCache.Set(alias, aliasTerms, cache.NoExpiration)
	indexTermsCache.Set(index, indexTerms, cache.NoExpiration)

	indexTermsJSON, err := json.Marshal(indexTerms)
	if err != nil {
		return err
	}
	return MStore.Set(aliasPrefix(index), indexTermsJSON)
}

func aliasPrefix(name string) string {
	return AliasPath + name
}
