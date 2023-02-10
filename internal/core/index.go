// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package core contains the implementation of Tatris core features
package core

import (
	"fmt"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/common/utils"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/manage"
	"github.com/tatris-io/tatris/internal/protocol"
	"go.uber.org/zap"

	"strings"
)

type Index struct {
	*protocol.Index
	Shards []*Shard `json:"shards"`
}

func (index *Index) GetName() string {
	return index.Name
}

func (index *Index) GetShardNum() int {
	return len(index.Shards)
}

func (index *Index) GetShards() []*Shard {
	return index.Shards
}

func (index *Index) GetShard(idx int) *Shard {
	return index.Shards[idx]
}

// GetShardByRouting
// TODO: build the real route, temporarily think that there is always only 1 shard
func (index *Index) GetShardByRouting() *Shard {
	for _, shard := range index.Shards {
		return shard
	}
	return nil
}

func (index *Index) GetReaderByTime(start, end int64) (indexlib.Reader, error) {
	segments := make([]string, 0)
	for _, shard := range index.Shards {
		for _, segment := range shard.Segments {
			if segment.MatchTime(start, end) {
				segments = append(segments, segment.GetName())
			}
		}
	}
	logger.Info(
		"find readers",
		zap.String("index", index.Name),
		zap.Int64("start", start),
		zap.Int64("end", end),
		zap.Int("size", len(segments)),
		zap.Any("segments", segments),
	)
	if len(segments) == 0 {
		return nil, errs.ErrNoSegmentMatched
	}
	config := &indexlib.BaseConfig{
		DataPath: consts.DefaultDataPath,
	}
	return manage.GetReader(config, index.Mappings, segments...)
}

func (index *Index) CheckMapping(docID string, doc protocol.Document) error {
	if index.Index == nil || index.Mappings == nil || index.Mappings.Properties == nil {
		return fmt.Errorf("doc %s mapping can not be nil", docID)
	}

	properties := index.Mappings.Properties
	dynamic := index.Mappings.Dynamic

	for k, v := range doc {
		// make sure field-level dynamic
		fieldDynamic := getFieldDynamic(dynamic, properties, k)
		// make sure field type, explicit type or dynamic type
		fieldType, err := getFieldType(fieldDynamic, properties, k, v)
		if err != nil {
			return fmt.Errorf(
				"doc %s fail to make sure field type of %s, field dynamic: %s, for %s",
				docID,
				k,
				fieldDynamic,
				err.Error(),
			)
		}

		if _, ok := properties[k]; !ok && strings.EqualFold(dynamic, consts.DynamicMappingMode) {
			// add field to properties
			p := protocol.Property{
				Type:    fieldType,
				Dynamic: consts.DynamicMappingMode,
			}
			properties[k] = p
		}
	}
	index.Mappings.Properties = properties
	return nil
}

func getFieldType(
	dynamic string,
	properties map[string]protocol.Property,
	fieldName string,
	value interface{},
) (string, error) {
	if property, ok := properties[fieldName]; ok {
		if validFieldType(property, value) {
			return property.Type, nil
		}
		return "", &errs.InvalidValueError{Field: fieldName, Value: value}
	}
	switch dynamic {
	case consts.DynamicMappingMode:
		return getDynamicFieldType(value)
	case consts.IgnoreMappingMode:
		return "", nil
	case consts.StrictMappingMode:
		return "", &errs.InvalidValueError{Field: fieldName, Value: value}
	default:
		return "", &errs.UnsupportedError{Desc: "dynamic mode", Value: dynamic}
	}
}

// Check that the field type specified in the property matches the value data type
func validFieldType(property protocol.Property, value interface{}) bool {
	switch property.Type {
	case "text", "match_only_text", "keyword", "constant_keyword":
		return utils.IsString(value)
	case "date":
		return utils.IsDateType(value)
	case "short", "byte", "integer", "long":
		return utils.IsInteger(value)
	case "float", "double":
		return utils.IsFloat(value)
	case "boolean":
		return utils.IsBool(value)
	default:
		return false
	}
}

func getDynamicFieldType(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		if utils.IsDateType(v) {
			return "date", nil
		}
		return "text", nil
	case bool:
		return "boolean", nil
	case int, int64:
		return "long", nil
	case float32, float64:
		return "double", nil
	default:
		return "", fmt.Errorf("unknown field type of %s", v)
	}
}

func getFieldDynamic(
	dynamic string,
	properties map[string]protocol.Property,
	fieldName string,
) string {
	if property, ok := properties[fieldName]; ok {
		if !strings.EqualFold(property.Dynamic, "") {
			return property.Dynamic
		}
	}
	return dynamic
}
