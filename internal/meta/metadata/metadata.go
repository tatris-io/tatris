// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package metadata

import (
	"encoding/json"

	"github.com/bobg/go-generics/slices"
	"github.com/patrickmn/go-cache"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/meta/metadata/storage"
	"github.com/tatris-io/tatris/internal/meta/metadata/storage/boltdb"
	"github.com/tatris-io/tatris/internal/protocol"
	"go.uber.org/zap"
)

const AliasPath = "/_alias/"
const IndexPath = "/_index/"

var (
	MStore          storage.MetaStore
	IndexCache      *cache.Cache
	AliasTermsCache *cache.Cache // alias -> []AliasTerm
	IndexTermsCache *cache.Cache // index -> []AliasTerm
)

func init() {
	var err error
	MStore, err = boltdb.Open()
	if err != nil {
		logger.Panic("init metastore failed", zap.Error(err))
	}

	IndexCache = cache.New(cache.NoExpiration, cache.NoExpiration)

	AliasTermsCache = cache.New(
		cache.NoExpiration,
		cache.NoExpiration,
	)
	IndexTermsCache = cache.New(
		cache.NoExpiration,
		cache.NoExpiration,
	)
	if err := loadAliases(); err != nil {
		logger.Panic("load alias failed", zap.Error(err))
	}
}

func loadAliases() error {
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
		AliasTermsCache.Set(alias, terms, cache.NoExpiration)
	}
	groupByIndex, err := slices.Group(terms, func(t *protocol.AliasTerm) (string, error) {
		return t.Index, nil
	})
	if err != nil {
		return err
	}
	for index, terms := range groupByIndex {
		IndexTermsCache.Set(index, terms, cache.NoExpiration)
	}
	return nil
}
