// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package core contains the implementation of Tatris core features
package core

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/jinzhu/now"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/log/logger"
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
	splits := make([]string, 0)
	indexes := make([]string, 0)
	for _, shard := range index.Shards {
		for _, segment := range shard.Segments {
			if segment.MatchTime(start, end) {
				indexes = append(indexes, segment.GetName())
				splits = append(splits, fmt.Sprintf("%d/%d", shard.ShardID, segment.SegmentID))
			}
		}
	}
	logger.Info(
		"find readers",
		zap.String("index", index.Name),
		zap.Int64("start", start),
		zap.Int64("end", end),
		zap.Int("size", len(indexes)),
		zap.Any("splits", splits),
	)
	config := &indexlib.BaseConfig{
		DataPath: consts.DefaultDataPath,
	}
	return manage.GetReader(config, indexes...)
}

func (index *Index) CheckMapping(docID string, doc map[string]interface{}) error {
	if err := index.tryCheckDataFieldType(doc); err != nil {
		return fmt.Errorf("illegal doc %s for %s", docID, err.Error())
	}
	return nil
}

func (index *Index) tryCheckDataFieldType(doc map[string]interface{}) error {

	if index.Index == nil || index.Mappings == nil || index.Mappings.Properties == nil {
		return errors.New("mapping can not be nil")
	}

	properties := index.Mappings.Properties
	dynamic := index.Mappings.Dynamic

	for k, v := range doc {
		fieldType, err := checkFieldType(dynamic, properties, k, v)
		if err != nil {
			if err := handleByPolicy(dynamic); err != nil {
				return err
			}
			continue
		}
		if p, ok := properties[k]; ok {
			if !hasConflict(p, fieldType) {
				continue
			}
			if err := handleByPolicy(p.Dynamic); err != nil {
				return fmt.Errorf(
					"inconsistent field type of %s, current: %s original: %s",
					k,
					fieldType,
					p.Type,
				)
			}
		} else if strings.EqualFold(dynamic, consts.DynamicMappingConfig) {
			// try to add the field type dynamically
			p = protocol.Property{
				Type:    fieldType,
				Dynamic: consts.DynamicMappingConfig,
			}
			properties[k] = p
		} else {
			// explicit mapping check
			if err := handleByPolicy(dynamic); err != nil {
				return err
			}
		}
	}
	index.Mappings.Properties = properties
	return nil
}

func hasConflict(p protocol.Property, fieldType string) bool {
	dynamic := p.Dynamic
	if strings.EqualFold(dynamic, "") || strings.EqualFold(dynamic, consts.DynamicMappingConfig) {
		return !strings.EqualFold(p.Type, fieldType)
	}
	if validTypes, ok := consts.ValidDynamicMappingTypes[p.Type]; ok {
		return !validTypes.Contains(fieldType)
	}
	// invalid p.Type that is not contained in ValidDynamicMappingTypes
	return true
}

func handleByPolicy(dynamic string) error {
	if strings.EqualFold(dynamic, consts.StrictMappingConfig) {
		return errors.New("reject doc for strict mode")
	}
	return nil
}

func checkFieldType(
	dynamic string,
	properties map[string]protocol.Property,
	key string,
	value interface{},
) (string, error) {
	if strings.EqualFold(dynamic, consts.DynamicMappingConfig) {
		switch v := value.(type) {
		case string:
			if isDateType(v) {
				return consts.DateMappingType, nil
			}
			return consts.TextMappingType, nil
		case bool:
			return consts.BooleanMappingType, nil
		case int, int64:
			return "long", nil
		case float32, float64:
			return "double", nil
		default:
			return consts.UnknownMappingType, fmt.Errorf("unknown field type of %s", v)
		}
	} else {
		typeOf := reflect.TypeOf(value)
		typeName := typeOf.Name()
		// explicit field property
		if p, ok := properties[key]; ok {
			// property type is valid config
			if set, ok := consts.ValidFieldTypes[p.Type]; ok {
				// field type can be correctly handled
				if set.Contains(typeName) {
					// valid date type or other types
					if !strings.EqualFold(p.Type, consts.DateMappingType) || isDateType(value.(string)) {
						return p.Type, nil
					}
				}
			}
		}
		return consts.UnknownMappingType, fmt.Errorf("unknown field type of %s", typeName)
	}
}

func isDateType(value string) bool {
	_, err := now.Parse(value)
	return err == nil
}
