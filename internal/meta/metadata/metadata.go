// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package metadata

import (
	"encoding/json"
	"sync"

	"github.com/bobg/go-generics/slices"
	"github.com/patrickmn/go-cache"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/meta/metadata/storage"
	"github.com/tatris-io/tatris/internal/meta/metadata/storage/boltdb"
	"github.com/tatris-io/tatris/internal/protocol"
	"go.uber.org/zap"
)

const AliasPath = "/_alias/"
const IndexPath = "/_index/"
const IndexTemplatePath = "/_index_template/"

type Metadata struct {
	// MStore completes direct access to metadata physical storage
	MStore storage.MetaStore
	// IndexCache caches { name -> Index }
	IndexCache *cache.Cache
	// AliasTermsCache caches { alias -> []AliasTerm }
	AliasTermsCache *cache.Cache
	// AliasTermsCache caches { index -> []AliasTerm }
	IndexTermsCache *cache.Cache
	// TemplateCache caches { name -> IndexTemplate }
	TemplateCache *cache.Cache
}

var metadata *Metadata
var _once sync.Once

func M() *Metadata {
	_once.Do(func() {
		metadata = &Metadata{}
		metadata.initMetadata()
	})
	return metadata
}

func (m *Metadata) initMetadata() {
	var err error
	m.MStore, err = boltdb.Open()
	if err != nil {
		logger.Panic("init metastore failed", zap.Error(err))
	}

	if err := m.loadIndexes(); err != nil {
		logger.Panic("load indexes failed", zap.Error(err))
	}

	if err := m.loadAliases(); err != nil {
		logger.Panic("load aliases failed", zap.Error(err))
	}

	if err := m.loadIndexTemplates(); err != nil {
		logger.Panic("load index templates failed", zap.Error(err))
	}
}

func (m *Metadata) loadIndexes() error {
	m.IndexCache = cache.New(
		cache.NoExpiration,
		cache.NoExpiration,
	)
	bytesMap, err := m.MStore.List(IndexPath)
	if err != nil {
		return err
	}
	for _, bytes := range bytesMap {
		index := &core.Index{}
		if err := json.Unmarshal(bytes, index); err != nil {
			return err
		}
		shards := index.Shards
		if len(shards) > 0 {
			for _, shard := range shards {
				shard.Index = index
				segments := shard.Segments
				if len(segments) > 0 {
					for _, segment := range segments {
						segment.Shard = shard
					}
				}
			}
		}
		m.IndexCache.Set(index.Name, index, cache.NoExpiration)
	}
	return nil
}

func (m *Metadata) loadAliases() error {
	m.AliasTermsCache = cache.New(
		cache.NoExpiration,
		cache.NoExpiration,
	)
	m.IndexTermsCache = cache.New(
		cache.NoExpiration,
		cache.NoExpiration,
	)
	bytesMap, err := m.MStore.List(AliasPath)
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
		m.AliasTermsCache.Set(alias, terms, cache.NoExpiration)
	}
	groupByIndex, err := slices.Group(terms, func(t *protocol.AliasTerm) (string, error) {
		return t.Index, nil
	})
	if err != nil {
		return err
	}
	for index, terms := range groupByIndex {
		m.IndexTermsCache.Set(index, terms, cache.NoExpiration)
	}
	return nil
}

func (m *Metadata) loadIndexTemplates() error {
	m.TemplateCache = cache.New(
		cache.NoExpiration,
		cache.NoExpiration,
	)
	bytesMap, err := m.MStore.List(IndexTemplatePath)
	if err != nil {
		return err
	}
	for _, bytes := range bytesMap {
		template := &protocol.IndexTemplate{}
		if err := json.Unmarshal(bytes, template); err != nil {
			return err
		}
		m.TemplateCache.Set(template.Name, template, cache.NoExpiration)
	}
	return nil
}
