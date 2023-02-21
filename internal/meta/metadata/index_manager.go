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

const (
	MaxNumberOfShards   = 100
	MaxNumberOfReplicas = 5
)

var indexCache = cache.New(cache.NoExpiration, cache.NoExpiration)

func LoadIndexes() error {
	bytesMap, err := MStore.List(IndexPath)
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
		indexCache.Set(index.Name, index, cache.NoExpiration)
	}
	return nil
}

func CreateIndex(index *core.Index) error {
	FillIndexAsDefault(index.Index)
	if err := CheckIndexValid(index); err != nil {
		return err
	}
	buildIndex(index)
	logger.Info("create index", zap.Any("index", index))
	return SaveIndex(index)
}

func SaveIndex(index *core.Index) error {
	json, err := json.Marshal(index)
	if err != nil {
		return err
	}
	indexCache.Set(index.Name, index, cache.NoExpiration)
	return MStore.Set(indexPrefix(index.Name), json)
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
	return nil, &errs.IndexNotFoundError{Index: indexName}
}

func DeleteIndex(indexName string) error {
	index, err := GetIndex(indexName)
	if err != nil {
		return err
	}
	// first set the cache disable, then all requests for this index will get a 404
	indexCache.Delete(indexName)
	// close the index and its components (shards, segments, wals ...)
	err = index.Close()
	if err != nil {
		return err
	}
	// remove aliases
	err = RemoveAliasesByIndex(indexName)
	if err != nil {
		return err
	}
	// remove the index from metastore
	return MStore.Delete(indexPrefix(indexName))
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

func FillIndexAsDefault(index *protocol.Index) {
	if index.Mappings == nil {
		index.Mappings = &protocol.Mappings{}
	}
	if index.Mappings.Dynamic == "" {
		index.Mappings.Dynamic = consts.DynamicMappingMode
	}
	if index.Settings == nil {
		index.Settings = &protocol.Settings{NumberOfShards: 1, NumberOfReplicas: 1}
	}
}

func CheckIndexValid(index *core.Index) error {
	err := CheckSettings(index.Index.Settings)
	if err != nil {
		return err
	}
	err = CheckMappings(index.Index.Mappings)
	if err != nil {
		return err
	}
	return nil
}

func CheckSettings(settings *protocol.Settings) error {
	if settings == nil {
		return errs.ErrEmptySettings
	}
	if settings.NumberOfShards <= 0 || settings.NumberOfShards > MaxNumberOfShards {
		return &errs.InvalidRangeError{
			Desc:  "settings.NumberOfShards",
			Value: settings.NumberOfShards,
			Left:  1,
			Right: MaxNumberOfShards,
		}
	}
	if settings.NumberOfReplicas <= 0 || settings.NumberOfReplicas > MaxNumberOfReplicas {
		return &errs.InvalidRangeError{
			Desc:  "settings.NumberOfReplicas",
			Value: settings.NumberOfReplicas,
			Left:  1,
			Right: MaxNumberOfReplicas,
		}
	}
	return nil
}

func CheckMappings(mappings *protocol.Mappings) error {
	if mappings == nil {
		return errs.ErrEmptyMappings
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
		return &errs.InvalidFieldError{Field: consts.IDField, Message: "build-in field"}
	}
	properties[consts.IDField] = protocol.Property{
		Type:    consts.KeywordMappingType,
		Dynamic: consts.StrictMappingMode,
	}
	_, exist = properties[consts.TimestampField]
	if exist {
		return &errs.InvalidFieldError{Field: consts.TimestampField, Message: "build-in field"}
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

func indexPrefix(name string) string {
	return IndexPath + name
}
