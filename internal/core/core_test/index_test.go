// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package core_test

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/core/config"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
	"github.com/tatris-io/tatris/internal/query"
	"github.com/tatris-io/tatris/test/ut/prepare"
)

func TestIndex(t *testing.T) {

	// prepare
	start := time.Now()
	index, docs, err := prepare.CreateIndexAndDocs(start.Format(time.RFC3339Nano))
	if err != nil {
		t.Fatalf("prepare docs fail: %s", err.Error())
	}

	// test
	t.Run("test_index", func(t *testing.T) {
		index, err := metadata.GetIndex(index.Name)
		assert.NoError(t, err)
		assert.NotNil(t, index)
		assert.Equal(t, index.Settings.NumberOfShards, index.GetShardNum())
		assert.Equal(t, index.Settings.NumberOfShards, len(index.GetShards()))
		for i := 0; i < index.GetShardNum(); i++ {
			assert.NotNil(t, index.GetShard(i))
		}
		assert.NotNil(t, index.GetShardByRouting())
		reader, err := index.GetReaderByTime(start.Unix(), time.Now().UnixMilli())
		if reader != nil {
			defer reader.Close()
		}
		assert.NoError(t, err)
		assert.Equal(
			t,
			(int)(math.Ceil((float64(len(docs)))/(float64(config.Cfg.Segment.MatureThreshold)))),
			reader.Count(),
		)

		resp, err := query.SearchDocs(
			index, protocol.QueryRequest{
				Index: index.Name,
				Query: protocol.Query{Term: protocol.Term{"name": "elasticsearch"}},
				Size:  20,
			},
		)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(resp.Hits.Hits))
	})
}

func TestMapping(t *testing.T) {
	tests := []struct {
		name  string
		docs  []protocol.Document
		index *core.Index
	}{
		{
			name: "invalid_empty_index",
			docs: []protocol.Document{
				{
					"string_key": "string_value",
					"date_key":   "2023-01-28 12:42:00",
				},
			},
			index: &core.Index{},
		},
		{
			name: "invalid_dynamic_mapping",
			docs: []protocol.Document{
				{
					"string_key": "string_value",
					"date_key":   "2023-01-28 12:42:00",
				},
				{
					"string_key": "string_value",
					"date_key":   "invalid field",
				},
			},
			index: &core.Index{
				Index: &protocol.Index{
					Mappings: &protocol.Mappings{
						Dynamic: consts.DynamicMappingMode,
						Properties: map[string]protocol.Property{
							"date_key": {Dynamic: consts.StrictMappingMode},
						},
					},
				},
			},
		},
		{
			name: "valid_explicit_mapping",
			docs: []protocol.Document{
				{
					"string_key": "string_value",
					"date_key":   "2023-01-28 12:42:00",
				},
			},
			index: &core.Index{
				Index: &protocol.Index{
					Mappings: &protocol.Mappings{
						Dynamic: consts.IgnoreMappingMode,
						Properties: map[string]protocol.Property{
							"string_key": {Type: "text"},
							"date_key":   {Type: "date"},
						},
					},
				},
			},
		},
		{
			name: "valid_explicit_mapping",
			docs: []protocol.Document{
				{
					"string_key": "string_value",
					"date_key":   "2023-01-28 12:42:00",
				},
				{
					"string_key": "string_value",
					"date_key":   "valid field",
				},
			},
			index: &core.Index{
				Index: &protocol.Index{
					Mappings: &protocol.Mappings{
						Dynamic: consts.IgnoreMappingMode,
						Properties: map[string]protocol.Property{
							"string_key": {Type: "text"},
							"date_key":   {Type: "keyword"},
						},
					},
				},
			},
		},
		{
			name: "invalid_explicit_mapping",
			docs: []protocol.Document{
				{
					"string_key": "string_value",
					"date_key":   "2023-01-28 12:42:00",
				},
				{
					"string_key": "string_value",
					"date_key":   "invalid field",
				},
			},
			index: &core.Index{
				Index: &protocol.Index{
					Mappings: &protocol.Mappings{
						Dynamic: consts.IgnoreMappingMode,
						Properties: map[string]protocol.Property{
							"string_key": {Type: "text"},
							"date_key":   {Type: "date"},
						},
					},
				},
			},
		},
		{
			name: "invalid_explicit_mapping",
			docs: []protocol.Document{
				{
					"string_key": "string_value",
					"date_key":   "2023-01-28 12:42:00",
				},
				{
					"string_key": "string_value",
					"date_key":   "invalid field",
				},
			},
			index: &core.Index{
				Index: &protocol.Index{
					Mappings: &protocol.Mappings{
						Dynamic: consts.StrictMappingMode,
						Properties: map[string]protocol.Property{
							"string_key": {Type: "text"},
							"date_key":   {Type: "date"},
						},
					},
				},
			},
		},
		{
			name: "valid_dynamic_numeric_mapping",
			docs: []protocol.Document{
				{
					"long_key":    111111111111111,
					"integer_key": 1,
					"float_key":   float32(1.1),
					"double_key":  1.111111111111111,
				},
			},
			index: &core.Index{
				Index: &protocol.Index{
					Mappings: &protocol.Mappings{
						Dynamic:    consts.DynamicMappingMode,
						Properties: map[string]protocol.Property{},
					},
				},
			},
		},
		{
			name: "valid_explicit_numeric_mapping",
			docs: []protocol.Document{
				{
					"long_key":    111111111111111,
					"integer_key": 1,
					"float_key":   float32(1.1),
					"double_key":  1.111111111111111,
				},
			},
			index: &core.Index{
				Index: &protocol.Index{
					Mappings: &protocol.Mappings{
						Dynamic: consts.StrictMappingMode,
						Properties: map[string]protocol.Property{
							"long_key":    {Type: "long"},
							"integer_key": {Type: "integer"},
							"float_key":   {Type: "float"},
							"double_key":  {Type: "double"},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, doc := range tt.docs {
				testErr := tt.index.CheckMapping(doc)
				if strings.HasPrefix(tt.name, "valid_") {
					assert.NoError(t, testErr)
					if strings.HasPrefix(tt.name, "valid_dynamic_numeric") {
						assert.Equal(t, "double", tt.index.Mappings.Properties["float_key"].Type)
						assert.Equal(t, "double", tt.index.Mappings.Properties["double_key"].Type)
						assert.Equal(t, "long", tt.index.Mappings.Properties["long_key"].Type)
						assert.Equal(t, "long", tt.index.Mappings.Properties["integer_key"].Type)
					} else if strings.HasPrefix(tt.name, "valid_explicit_numeric") {
						assert.Equal(t, "float", tt.index.Mappings.Properties["float_key"].Type)
						assert.Equal(t, "double", tt.index.Mappings.Properties["double_key"].Type)
						assert.Equal(t, "long", tt.index.Mappings.Properties["long_key"].Type)
						assert.Equal(t, "integer", tt.index.Mappings.Properties["integer_key"].Type)
					}
				} else if strings.HasPrefix(tt.name, "invalid_") && i > 0 {
					assert.NotNil(t, testErr)
				}
			}
		})
	}
}
