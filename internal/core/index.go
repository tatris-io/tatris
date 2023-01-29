// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package core contains the implementation of Tatris core features
package core

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/protocol"
	"go.uber.org/zap"

	"strings"
	"time"
)

type Index struct {
	*protocol.Index
	Shards []*Shard `json:"shards"`
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

func (index *Index) GetReadersByTime(start, end int64) ([]indexlib.Reader, error) {
	splits := make([]string, 0)
	readers := make([]indexlib.Reader, 0)
	for _, shard := range index.Shards {
		for _, segment := range shard.Segments {
			if segment.MatchTime(start, end) {
				reader, err := segment.GetReader()
				if err != nil {
					return nil, err
				}
				splits = append(splits, fmt.Sprintf("%d/%d", shard.ShardID, segment.SegmentID))
				readers = append(readers, reader)
			}
		}
	}
	logger.Info(
		"find readers",
		zap.String("index", index.Name),
		zap.Int64("start", start),
		zap.Int64("end", end),
		zap.Int("size", len(readers)),
		zap.Any("splits", splits),
	)
	return readers, nil
}

func (index *Index) CheckMapping(docID string, doc map[string]interface{}) error {
	if err := index.tryCheckDataFieldType(doc); err != nil {
		return fmt.Errorf("illegal doc %s for %s", docID, err.Error())
	}
	return nil
}

func (index *Index) tryCheckDataFieldType(doc map[string]interface{}) error {

	if index.Index == nil {
		index.Index = &protocol.Index{
			Mappings: &protocol.Mappings{
				Dynamic:        true,
				RejectedPolicy: "ignore",
			},
		}
	}
	if index.Mappings == nil {
		index.Mappings = &protocol.Mappings{
			Dynamic:        true,
			RejectedPolicy: "ignore",
		}
	}
	if index.Mappings.Properties == nil {
		index.Mappings.Properties = make(map[string]protocol.Property, 0)
	}
	properties := index.Mappings.Properties
	dynamic := index.Mappings.Dynamic
	policy := index.Mappings.RejectedPolicy

	for k, v := range doc {
		fieldType, err := checkFieldType(dynamic, properties, k, v)
		if err != nil {
			if err := handleByPolicy(policy, k, doc); err != nil {
				return err
			}
			continue
		}
		if p, ok := properties[k]; ok {
			if strings.EqualFold(p.Type, fieldType) {
				continue
			}
			return fmt.Errorf(
				"inconsistent field type of %s, current: %s original: %s",
				k,
				fieldType,
				p.Type,
			)
		} else if dynamic {
			// try to add the field type dynamically
			p = protocol.Property{Type: fieldType}
			properties[k] = p
		} else {
			// explicit mapping check
			if err := handleByPolicy(policy, k, doc); err != nil {
				return err
			}
		}
	}
	index.Mappings.Properties = properties
	return nil
}

func handleByPolicy(policy string, k string, doc map[string]interface{}) error {
	switch policy {
	case "ignore":
		delete(doc, k)
	case "abort":
		return errors.New("reject doc for abort policy")
	default:
		return fmt.Errorf("unknown policy %s", policy)
	}
	return nil
}

func checkFieldType(
	dynamic bool,
	properties map[string]protocol.Property,
	key string,
	value interface{},
) (string, error) {
	if dynamic {
		switch v := value.(type) {
		case string:
			if isDateType(v) {
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
			return "other", fmt.Errorf("unknown field type of %s", v)
		}
	} else {
		typeOf := reflect.TypeOf(value)
		typeName := typeOf.Name()
		if p, ok := properties[key]; ok {
			switch p.Type {
			case "text", "keyword":
				if strings.EqualFold(typeName, "string") {
					return p.Type, nil
				}
			case "date":
				if strings.EqualFold(typeName, "string") && isDateType(value.(string)) {
					return p.Type, nil
				}
			case "long":
				if strings.HasPrefix(typeName, "int") {
					return p.Type, nil
				}
			case "integer":
				if strings.HasPrefix(typeName, "int") && !strings.EqualFold(typeName, "int64") {
					return p.Type, nil
				}
			case "double":
				if strings.HasPrefix(typeName, "float") {
					return p.Type, nil
				}
			case "float":
				if strings.HasPrefix(typeName, "float") && !strings.EqualFold(typeName, "float64") {
					return p.Type, nil
				}
			case "byte":
				if strings.EqualFold(typeName, "byte") || strings.EqualFold(typeName, "int") {
					return p.Type, nil
				}
			}
		}
		return "other", fmt.Errorf("unknown field type of %s", typeName)
	}
}

func isDateType(value string) bool {
	layout, layoutErr := detectTimeLayout(value)
	if layoutErr != nil {
		logger.Warn(layoutErr.Error())
		return false
	}
	_, err := time.Parse(layout, value)
	return err == nil
}

func detectTimeLayout(value string) (string, error) {
	if len(value) == 19 {
		if strings.Index(value, " ") == 10 {
			return "2006-01-02 15:04:05", nil
		} else if strings.Index(value, "T") == 10 {
			return "2006-01-02T15:04:05", nil
		}
	} else if len(value) == 25 && strings.Index(value, "T") == 10 {
		return "2006-01-02T15:04:05Z07:00", nil
	} else if len(value) == 29 && strings.Index(value, "T") == 10 && strings.Index(value, ".") == 19 {
		return "2006-01-02T15:04:05.999Z07:00", nil
	}
	return "", fmt.Errorf("unsupported time layout of %s", value)
}
