// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package core_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/protocol"
)

const (
	stringKey  = "string_key"
	dateKey    = "date_key"
	longKey    = "long_key"
	integerKey = "integer_key"
	floatKey   = "float_key"
	doubleKey  = "double_key"
)

func TestCheckDocuments(t *testing.T) {
	tests := []struct {
		name   string
		docs   []protocol.Document
		index  *core.Index
		result bool
	}{
		{
			name: "empty_mappings",
			docs: []protocol.Document{
				{
					consts.IDField:        "123456789",
					consts.TimestampField: "2023-02-22T19:47:36.499723+08:00",
					stringKey:             "string_value",
					dateKey:               "2023-01-28 12:42:00",
				},
			},
			index:  &core.Index{Index: &protocol.Index{Name: "empty_mappings"}},
			result: false,
		},
		{
			name: "explicit_mappings",
			docs: []protocol.Document{
				{
					consts.IDField:        "123456789",
					consts.TimestampField: "2023-02-22T19:47:36.499723+08:00",
					stringKey:             "string_value",
					dateKey:               "2023-01-28 12:42:00",
				},
			},
			index: &core.Index{
				Index: &protocol.Index{
					Name: "explicit_mappings",
					Mappings: &protocol.Mappings{
						Dynamic: consts.DynamicMappingMode,
						Properties: map[string]*protocol.Property{
							consts.IDField:        {Type: consts.Keyword},
							consts.TimestampField: {Type: consts.Date},
							stringKey:             {Type: consts.Text},
							dateKey:               {Type: consts.Date},
						},
					},
				},
			},
			result: true,
		},
		{
			name: "explicit_mappings_with_invalid_value",
			docs: []protocol.Document{
				{
					consts.IDField:        "123456789",
					consts.TimestampField: "2023-02-22T19:47:36.499723+08:00",
					stringKey:             "string_value",
					dateKey:               "2023-01-28 12:42:00",
				},
				{
					consts.IDField:        "123456789",
					consts.TimestampField: "2023-02-22T19:47:36.499723+08:00",
					stringKey:             "string_value",
					dateKey:               "invalid field",
				},
			},
			index: &core.Index{
				Index: &protocol.Index{
					Name: "explicit_mappings_with_invalid_value",
					Mappings: &protocol.Mappings{
						Dynamic: consts.IgnoreMappingMode,
						Properties: map[string]*protocol.Property{
							consts.IDField:        {Type: consts.Keyword},
							consts.TimestampField: {Type: consts.Date},
							stringKey:             {Type: consts.Text},
							dateKey:               {Type: consts.Date},
						},
					},
				},
			},
			result: false,
		},
		{
			name: "strict_mappings_with_unknown_field",
			docs: []protocol.Document{
				{
					consts.IDField:        "123456789",
					consts.TimestampField: "2023-02-22T19:47:36.499723+08:00",
					stringKey:             "string_value",
					dateKey:               "2023-01-28 12:42:00",
				},
			},
			index: &core.Index{
				Index: &protocol.Index{
					Name: "strict_mappings_with_unknown_field",
					Mappings: &protocol.Mappings{
						Dynamic: consts.DynamicMappingMode,
						Properties: map[string]*protocol.Property{
							consts.IDField:        {Type: consts.Keyword},
							consts.TimestampField: {Type: consts.Date},
							dateKey:               {Dynamic: consts.StrictMappingMode},
						},
					},
				},
			},
			result: false,
		},
		{
			name: "dynamic_mappings",
			docs: []protocol.Document{
				{
					consts.IDField:        "123456789",
					consts.TimestampField: "2023-02-22T19:47:36.499723+08:00",
					longKey:               111111111111111,
					integerKey:            1,
					floatKey:              float32(1.1),
					doubleKey:             1.111111111111111,
				},
			},
			index: &core.Index{
				Index: &protocol.Index{
					Name: "dynamic_mappings",
					Mappings: &protocol.Mappings{
						Dynamic: consts.DynamicMappingMode,
						Properties: map[string]*protocol.Property{
							consts.IDField:        {Type: consts.Keyword},
							consts.TimestampField: {Type: consts.Date},
						},
					},
				},
			},
			result: true,
		},
		{
			name: "explicit_dynamic_mappings",
			docs: []protocol.Document{
				{
					consts.IDField:        "123456789",
					consts.TimestampField: "2023-02-22T19:47:36.499723+08:00",
					longKey:               111111111111111,
					integerKey:            1,
					floatKey:              float32(1.1),
					doubleKey:             1.111111111111111,
				},
			},
			index: &core.Index{
				Index: &protocol.Index{
					Name: "explicit_dynamic_mappings",
					Mappings: &protocol.Mappings{
						Dynamic: consts.StrictMappingMode,
						Properties: map[string]*protocol.Property{
							consts.IDField:        {Type: consts.Keyword},
							consts.TimestampField: {Type: consts.Date},
							longKey:               {Type: consts.Long},
							integerKey:            {Type: consts.Integer},
							floatKey:              {Type: consts.Float},
							doubleKey:             {Type: consts.Double},
						},
					},
				},
			},
			result: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testErr := core.BuildDocuments(tt.index, tt.docs)
			if tt.result {
				assert.NoError(t, testErr)
				checkProperties(t, tt.index.Mappings.Dynamic, tt.index.Mappings.Properties)
			} else {
				assert.NotNil(t, testErr)
			}
		})
	}
}

func checkProperties(t *testing.T, dynamic string, properties map[string]*protocol.Property) {
	if dynamic == consts.DynamicMappingMode {
		if property, ok := properties[floatKey]; ok {
			assert.Equal(t, consts.Double, property.Type)
		}
		if property, ok := properties[doubleKey]; ok {
			assert.Equal(t, consts.Double, property.Type)
		}
		if property, ok := properties[longKey]; ok {
			assert.Equal(t, consts.Long, property.Type)
		}
		if property, ok := properties[integerKey]; ok {
			assert.Equal(t, consts.Long, property.Type)
		}
	} else {
		if property, ok := properties[floatKey]; ok {
			assert.Equal(t, consts.Float, property.Type)
		}
		if property, ok := properties[doubleKey]; ok {
			assert.Equal(t, consts.Double, property.Type)
		}
		if property, ok := properties[longKey]; ok {
			assert.Equal(t, consts.Long, property.Type)
		}
		if property, ok := properties[integerKey]; ok {
			assert.Equal(t, consts.Integer, property.Type)
		}
	}
}
