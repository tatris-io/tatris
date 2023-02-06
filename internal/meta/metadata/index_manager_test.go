// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package metadata is about the management of metadata (i.e. index)
package metadata

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/protocol"
)

type testItem struct {
	Index protocol.Index
	Res   bool
}

func TestManager(t *testing.T) {
	t.Run("check_param", func(t *testing.T) {
		params := `[
		{"Res":true ,"Index":{"settings":{"number_of_shards":3,"number_of_replicas":1},"mappings":{"properties":{"name":{"type":"keyword"}}}}},
		{"Res":true ,"Index":{"settings":{"number_of_shards":3,"number_of_replicas":1},"mappings":{"properties":{"name":{"type":"text"}}}}},
		{"Res":true ,"Index":{"settings":{"number_of_shards":3,"number_of_replicas":1},"mappings":{"properties":{"name":{"type":"INTEGER"}}}}},
		{"Res":true ,"Index":{"settings":{"number_of_shards":3,"number_of_replicas":1},"mappings":{"properties":{"name":{"type":"long"}}}}},
		{"Res":true ,"Index":{"settings":{"number_of_shards":3,"number_of_replicas":1},"mappings":{"properties":{"name":{"type":"FLOAT"}}}}},
		{"Res":true ,"Index":{"settings":{"number_of_shards":3,"number_of_replicas":1},"mappings":{"properties":{"name":{"type":"double"}}}}},
		{"Res":true ,"Index":{"settings":{"number_of_shards":3,"number_of_replicas":1},"mappings":{"properties":{"name":{"type":"BOOLEAN"}}}}},
		{"Res":true ,"Index":{"settings":{"number_of_shards":3,"number_of_replicas":1},"mappings":{"properties":{"name":{"type":"date"}}}}},
		{"Res":true ,"Index":{"settings":{"number_of_shards":3,"number_of_replicas":1},"mappings":{"properties":{"name":{"type":"dAtE"}}}}},
		{"Res":false},
		{"Res":true ,"Index":{"settings":{"number_of_shards":3,"number_of_replicas":1},"mappings":{}}},
		{"Res":false ,"Index":{"settings":{"number_of_shards":3,"number_of_replicas":1},"mappings":{"properties":{"name":{"type":"keyword"},"age":{"type":"string"}}}}},
		{"Res":false ,"Index":{"settings":{"number_of_shards":3,"number_of_replicas":1},"mappings":{"properties":{"name":{"type":"bool"},"age":{"type":"int"}}}}}
	]`
		var items []testItem
		err := json.Unmarshal([]byte(params), &items)
		if err != nil {
			t.Error(err)
			return
		}
		for i, item := range items {
			err := checkParam(&item.Index)
			comparison := err == nil
			if !comparison {
				t.Logf("item %d error : %s", i, err)
			}
			assert.Equal(t, comparison, item.Res)
		}
	})

}

func TestDynamicMappingCheck(t *testing.T) {
	tests := []struct {
		name     string
		mappings *protocol.Mappings
	}{
		{
			name:     "empty_mapping",
			mappings: &protocol.Mappings{},
		},
		{
			name: "dynamic_mapping",
			mappings: &protocol.Mappings{
				Dynamic: "true",
			},
		},
		{
			name: "invalid_explicit_mapping",
			mappings: &protocol.Mappings{
				Dynamic: "false",
			},
		},
		{
			name: "valid_explicit_mapping",
			mappings: &protocol.Mappings{
				Dynamic:    "false",
				Properties: map[string]protocol.Property{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testErr := checkMapping(tt.mappings)
			if strings.HasPrefix(tt.name, "valid_") {
				assert.NoError(t, testErr)
			} else if strings.HasPrefix(tt.name, "invalid_") {
				assert.True(t, testErr != nil)
			}
		})
	}
}
