// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package metadata is about the management of metadata (i.e. index)
package metadata

import (
	"encoding/json"
	"strings"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/patrickmn/go-cache"

	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/common/log/logger"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/protocol"
)

var indexCache = cache.New(cache.NoExpiration, cache.NoExpiration)

func CreateIndex(index *core.Index) error {
	err := checkParam(index.Index)
	buildIndex(index)
	if err != nil {
		return err
	}
	logger.Info("create index", zap.Any("index", index))
	return SaveIndex(index)
}

func SaveIndex(index *core.Index) error {
	json, err := json.Marshal(index)
	if err != nil {
		return err
	}
	indexCache.Set(index.Name, index, cache.NoExpiration)
	return MStore.Set(prefix(index.Name), json)
}

func GetShard(indexName string, shardID int) (*core.Shard, error) {
	index, err := GetIndex(indexName)
	if err != nil {
		return nil, err
	}
	if index == nil {
		return nil, nil
	}
	shard := index.GetShard(shardID)
	if shard == nil {
		return nil, &errs.ShardNotFoundError{Index: indexName, Shard: shardID}
	}
	return shard, nil
}

func GetIndex(indexName string) (*core.Index, error) {
	var index *core.Index
	cachedIndex, found := indexCache.Get(indexName)
	if found {
		index = cachedIndex.(*core.Index)
		return index, nil
	}
	// load
	if b, err := MStore.Get(prefix(indexName)); err != nil {
		return nil, err
	} else if b == nil {
		return nil, &errs.IndexNotFoundError{Index: indexName}
	} else {
		index := &core.Index{}
		if err := json.Unmarshal(b, index); err != nil {
			return nil, err
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
		indexCache.Set(index.Name, index, cache.NoExpiration)
		return index, nil
	}
}

func DeleteIndex(indexName string) error {
	indexCache.Delete(indexName)
	return MStore.Delete(prefix(indexName))
}

func buildIndex(index *core.Index) {
	numberOfShards := index.Settings.NumberOfShards
	shards := make([]*core.Shard, numberOfShards)
	for i := 0; i < numberOfShards; i++ {
		shards[i] = &core.Shard{}
		shards[i].ShardID = i
		shards[i].Index = index
		shards[i].Stat = core.ShardStat{}
	}
	index.Shards = shards
}

func checkParam(index *protocol.Index) error {
	mappings := index.Mappings
	if mappings == nil {
		return errs.ErrEmptyMappings
	}
	err := checkMapping(mappings)
	if err != nil {
		return err
	}
	return nil
}

func checkMapping(mappings *protocol.Mappings) error {
	if mappings.Dynamic == "" {
		mappings.Dynamic = consts.DynamicMappingMode
	}
	dynamic := strings.EqualFold(mappings.Dynamic, consts.DynamicMappingMode)
	properties := &mappings.Properties
	if *properties == nil {
		if dynamic {
			mappings.Properties = make(map[string]protocol.Property, 0)
		} else {
			return errs.ErrEmptyMappings
		}
	}
	err := checkReservedField(*properties)
	if err != nil {
		return err
	}
	for _, property := range *properties {
		err = checkType(property.Type)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkReservedField(properties map[string]protocol.Property) error {
	_, exist := properties[consts.IDField]
	if exist {
		return &errs.InvalidFieldError{Field: consts.IDField, Message: "build-it field"}
	}
	properties[consts.IDField] = protocol.Property{
		Type:    consts.KeywordMappingType,
		Dynamic: consts.StrictMappingMode,
	}
	_, exist = properties[consts.TimestampField]
	if exist {
		return &errs.InvalidFieldError{Field: consts.TimestampField, Message: "build-it field"}
	}
	properties[consts.TimestampField] = protocol.Property{
		Type:    consts.DateMappingType,
		Dynamic: consts.StrictMappingMode,
	}
	return nil
}

func checkType(paramType string) error {
	if _, ok := consts.MappingTypes[strings.ToLower(paramType)]; ok {
		return nil
	}
	return &errs.UnsupportedError{Desc: "field type", Value: paramType}
}

func prefix(name string) string {
	return IndexPath + name
}
