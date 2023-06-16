// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package metadata

import (
	"encoding/json"
	"fmt"
	"sync"

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
	// AliasTermsCache caches { index&alias -> AliasTerm }
	AliasTermsCache *cache.Cache
	// TemplateCache caches { name -> IndexTemplate }
	TemplateCache *cache.Cache
}

var metadata *Metadata
var _once sync.Once

// Instance lazily initializes a Metadata singleton and returns it
func Instance() *Metadata {
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

	if err := m.initialRevise(); err != nil {
		logger.Panic("revise meta failed", zap.Error(err))
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

// initialRevise is used to perform some necessary revision actions, such as:
// 1. marking the core.SegmentStatusWritable segment during the last process run as
// core.SegmentStatusReadonly, so that the writer can generate a new segment later.
func (m *Metadata) initialRevise() error {
	for _, item := range m.IndexCache.Items() {
		index := item.Object.(*core.Index)
		shards := index.Shards
		revised := false
		if len(shards) > 0 {
			for _, shard := range shards {
				shard.Index = index
				segments := shard.Segments
				if len(segments) > 0 {
					for _, segment := range segments {
						segment.Shard = shard
						// revise writable segments to read-only (mature)
						if segment.SegmentStatus != core.SegmentStatusReadonly {
							logger.Info(
								"revise segment status",
								zap.String("segment", segment.GetName()),
								zap.Uint8("from", segment.SegmentStatus),
								zap.Uint8("to", core.SegmentStatusReadonly),
							)
							segment.OnMature()
							revised = true
						}
					}
				}
			}
		}
		if revised {
			json, err := json.Marshal(index)
			if err != nil {
				return err
			}
			err = m.MStore.Set(indexPrefix(index.Name), json)
			if err != nil {
				return err
			}
			m.IndexCache.Set(index.Name, index, cache.NoExpiration)
		}
	}
	return nil
}

func (m *Metadata) loadAliases() error {
	m.AliasTermsCache = cache.New(
		cache.NoExpiration,
		cache.NoExpiration,
	)
	bytesMap, err := m.MStore.List(AliasPath)
	if err != nil {
		return err
	}
	terms := make([]*protocol.AliasTerm, 0)
	for _, bytes := range bytesMap {
		ts := &protocol.AliasTerm{}
		if err := json.Unmarshal(bytes, ts); err != nil {
			return err
		}
		terms = append(terms, ts)
	}
	for _, term := range terms {
		m.AliasTermsCache.Set(aliasTermKey(term.Index, term.Alias), term, cache.NoExpiration)
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

func aliasTermKey(index, alias string) string {
	return fmt.Sprintf("%s&&%s", index, alias)
}
