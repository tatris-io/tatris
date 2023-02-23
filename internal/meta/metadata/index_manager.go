// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package metadata is about the management of metadata (i.e. index)
package metadata

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tatris-io/tatris/internal/indexlib"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/patrickmn/go-cache"

	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/common/log/logger"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/protocol"
)

const (
	DefaultNumberOfShards   = 1
	MaxNumberOfShards       = 100
	DefaultNumberOfReplicas = 1
	MaxNumberOfReplicas     = 5
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
	template := FindTemplates(index.Name)
	BuildIndex(index, template)
	if template != nil && template.Template != nil && template.Template.Aliases != nil {
		for alias, term := range template.Template.Aliases {
			term.Index = index.Name
			term.Alias = alias
			if err := AddAlias(term); err != nil {
				return err
			}
		}
	}
	if err := CheckIndexValid(index); err != nil {
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

func BuildIndex(index *core.Index, template *protocol.IndexTemplate) {
	mappings := &protocol.Mappings{
		Dynamic:    consts.DynamicMappingMode,
		Properties: make(map[string]*protocol.Property),
	}
	settings := &protocol.Settings{
		NumberOfShards:   DefaultNumberOfShards,
		NumberOfReplicas: DefaultNumberOfReplicas,
	}
	// first, initialize mappings and settings with a template if it exists
	if template != nil {
		if template.Template != nil {
			if template.Template.Mappings != nil {
				if template.Template.Mappings.Dynamic != "" {
					mappings.Dynamic = template.Template.Mappings.Dynamic
				}
				for n, p := range template.Template.Mappings.Properties {
					mappings.Properties[n] = &protocol.Property{Type: p.Type, Dynamic: p.Dynamic}
				}
			}
			if template.Template.Settings != nil {
				settings.NumberOfShards = template.Template.Settings.NumberOfShards
				settings.NumberOfReplicas = template.Template.Settings.NumberOfReplicas
			}
		}
	}
	// then, use the passed index to assign the real value
	if index.Mappings != nil {
		if index.Mappings.Dynamic != "" {
			mappings.Dynamic = index.Mappings.Dynamic
		}
		for n, p := range index.Mappings.Properties {
			property := &protocol.Property{Type: p.Type}
			if p.Dynamic != "" {
				property.Dynamic = p.Dynamic
			} else {
				property.Dynamic = mappings.Dynamic
			}
			mappings.Properties[n] = property
		}
	}
	if index.Settings != nil {
		if index.Settings.NumberOfShards != 0 {
			settings.NumberOfShards = index.Settings.NumberOfShards
		}
		if index.Settings.NumberOfReplicas != 0 {
			settings.NumberOfReplicas = index.Settings.NumberOfReplicas
		}
	}
	index.Mappings = mappings
	index.Settings = settings
	// finally, build shards
	shards := make([]*core.Shard, index.Settings.NumberOfShards)
	for i := 0; i < index.Settings.NumberOfShards; i++ {
		shards[i] = &core.Shard{}
		shards[i].ShardID = i
		shards[i].Index = index
		shards[i].Stat = core.ShardStat{}
	}
	index.Shards = shards
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
	if mappings.Properties == nil {
		if dynamic {
			mappings.Properties = make(map[string]*protocol.Property, 0)
		} else {
			return errs.ErrEmptyMappings
		}
	}
	err := checkReservedField(mappings.Properties)
	if err != nil {
		return err
	}
	for _, property := range mappings.Properties {
		err = checkMappingType(property.Type)
		if err != nil {
			return err
		}
	}
	err = checkDynamicTemplates(mappings.DynamicTemplates)
	if err != nil {
		return err
	}
	return nil
}

func checkReservedField(properties map[string]*protocol.Property) error {
	IDField, exist := properties[consts.IDField]
	if exist {
		if !strings.EqualFold(IDField.Type, consts.MappingFieldTypeKeyword) {
			return &errs.InvalidFieldError{
				Field: consts.IDField,
				Message: fmt.Sprintf(
					"%s must be %s type",
					consts.IDField,
					consts.MappingFieldTypeKeyword,
				),
			}
		}
	} else {
		IDField = &protocol.Property{
			Type: consts.MappingFieldTypeKeyword,
		}
		properties[consts.IDField] = IDField
	}
	IDField.Dynamic = consts.StrictMappingMode

	TimestampField, exist := properties[consts.TimestampField]
	if exist {
		if !strings.EqualFold(TimestampField.Type, consts.MappingFieldTypeDate) {
			return &errs.InvalidFieldError{
				Field: consts.TimestampField,
				Message: fmt.Sprintf(
					"%s must be %s type",
					consts.TimestampField,
					consts.MappingFieldTypeDate,
				),
			}
		}
	} else {
		TimestampField = &protocol.Property{
			Type: consts.MappingFieldTypeDate,
		}
		properties[consts.TimestampField] = TimestampField
	}
	TimestampField.Dynamic = consts.StrictMappingMode
	return nil
}

func checkDynamicTemplates(templates []map[string]*protocol.DynamicTemplate) error {
	for _, template := range templates {
		for _, dt := range template {
			if dt.Mapping == nil {
				return errs.ErrNoMappingInDynamicTemplate
			}
			if err := checkMappingType(dt.Mapping.Type); err != nil {
				return err
			}
			if dt.MatchMappingType != "" && !consts.IsJSONFieldType(dt.MatchMappingType) {
				return errs.ErrInvalidJSONType
			}
		}
	}
	return nil
}

func checkMappingType(mappingType string) error {
	if ok, _ := indexlib.ValidateMappingType(strings.ToLower(mappingType)); ok {
		return nil
	}
	return &errs.UnsupportedError{Desc: "field type", Value: mappingType}
}

func indexPrefix(name string) string {
	return IndexPath + name
}
