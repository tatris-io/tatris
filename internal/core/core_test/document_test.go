// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package core_test

import (
	"testing"

	"github.com/tatris-io/tatris/internal/common/utils"

	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/protocol"
)

const (
	stringKey        = "string_key"
	stringKeyMatch   = "string_key_match"
	stringKeyUnmatch = "string_key_unmatch"
	dateKey          = "date_key"
	longKey          = "long_key"
	integerKey       = "integer_key"
	floatKey         = "float_key"
	doubleKey        = "double_key"
)

func TestCheckDocuments(t *testing.T) {
	tests := []struct {
		name          string
		docs          []protocol.Document
		index         *core.Index
		expectedTypes map[string]string
		result        bool
	}{
		{
			name: "empty_mappings",
			docs: []protocol.Document{
				{
					consts.IDField:        "123456789",
					consts.TimestampField: "2023-02-22T19:47:36.499723+08:00",
				},
			},
			index: &core.Index{Index: &protocol.Index{Name: "empty_mappings"}},
			expectedTypes: map[string]string{
				consts.IDField:        consts.MappingFieldTypeKeyword,
				consts.TimestampField: consts.MappingFieldTypeDate,
			},
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
							consts.IDField:        {Type: consts.MappingFieldTypeKeyword},
							consts.TimestampField: {Type: consts.MappingFieldTypeDate},
							stringKey:             {Type: consts.MappingFieldTypeText},
							dateKey:               {Type: consts.MappingFieldTypeDate},
						},
					},
				},
			},
			expectedTypes: map[string]string{
				consts.IDField:        consts.MappingFieldTypeKeyword,
				consts.TimestampField: consts.MappingFieldTypeDate,
				stringKey:             consts.MappingFieldTypeText,
				dateKey:               consts.MappingFieldTypeDate,
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
							consts.IDField:        {Type: consts.MappingFieldTypeKeyword},
							consts.TimestampField: {Type: consts.MappingFieldTypeDate},
							stringKey:             {Type: consts.MappingFieldTypeText},
							dateKey:               {Type: consts.MappingFieldTypeDate},
						},
					},
				},
			},
			expectedTypes: map[string]string{
				consts.IDField:        consts.MappingFieldTypeKeyword,
				consts.TimestampField: consts.MappingFieldTypeDate,
				stringKey:             consts.MappingFieldTypeText,
				dateKey:               consts.MappingFieldTypeDate,
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
							consts.IDField:        {Type: consts.MappingFieldTypeKeyword},
							consts.TimestampField: {Type: consts.MappingFieldTypeDate},
							dateKey:               {Dynamic: consts.StrictMappingMode},
						},
					},
				},
			},
			expectedTypes: map[string]string{
				consts.IDField:        consts.MappingFieldTypeKeyword,
				consts.TimestampField: consts.MappingFieldTypeDate,
				stringKey:             consts.MappingFieldTypeText,
				dateKey:               consts.MappingFieldTypeDate,
			},
			result: false,
		},
		{
			name: "dynamic_mappings",
			docs: []protocol.Document{
				{
					consts.IDField:        "123456789",
					consts.TimestampField: "2023-02-22T19:47:36.499723+08:00",
					stringKey:             "string_value",
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
							consts.IDField:        {Type: consts.MappingFieldTypeKeyword},
							consts.TimestampField: {Type: consts.MappingFieldTypeDate},
						},
					},
				},
			},
			expectedTypes: map[string]string{
				consts.IDField:        consts.MappingFieldTypeKeyword,
				consts.TimestampField: consts.MappingFieldTypeDate,
				stringKey:             consts.MappingFieldTypeText,
				longKey:               consts.MappingFieldTypeLong,
				integerKey:            consts.MappingFieldTypeLong,
				floatKey:              consts.MappingFieldTypeFloat,
				doubleKey:             consts.MappingFieldTypeFloat,
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
							consts.IDField:        {Type: consts.MappingFieldTypeKeyword},
							consts.TimestampField: {Type: consts.MappingFieldTypeDate},
							longKey:               {Type: consts.MappingFieldTypeLong},
							integerKey:            {Type: consts.MappingFieldTypeInteger},
							floatKey:              {Type: consts.MappingFieldTypeFloat},
							doubleKey:             {Type: consts.MappingFieldTypeDouble},
						},
					},
				},
			},
			expectedTypes: map[string]string{
				consts.IDField:        consts.MappingFieldTypeKeyword,
				consts.TimestampField: consts.MappingFieldTypeDate,
				stringKey:             consts.MappingFieldTypeText,
				longKey:               consts.MappingFieldTypeLong,
				integerKey:            consts.MappingFieldTypeInteger,
				floatKey:              consts.MappingFieldTypeFloat,
				doubleKey:             consts.MappingFieldTypeDouble,
			},
			result: true,
		},
		{
			name: "dynamic_template_match_mapping_type",
			docs: []protocol.Document{
				{
					consts.IDField:        "123456789",
					consts.TimestampField: "2023-02-22T19:47:36.499723+08:00",
					stringKey:             "string_value",
					dateKey:               "2023-01-28 12:42:00",
					longKey:               1,
					doubleKey:             1.111111111111111,
				},
			},
			index: &core.Index{
				Index: &protocol.Index{
					Name: "dynamic_template_match_mapping_type",
					Mappings: &protocol.Mappings{
						Dynamic: consts.DynamicMappingMode,
						DynamicTemplates: []map[string]*protocol.DynamicTemplate{
							{"string_as_keyword": &protocol.DynamicTemplate{
								MatchMappingType: consts.JSONFieldTypeString,
								Mapping: &protocol.DynamicTemplateMapping{
									Type: consts.MappingFieldTypeKeyword,
								},
							}},
							{"date_as_keyword": &protocol.DynamicTemplate{
								MatchMappingType: consts.JSONFieldTypeDate,
								Mapping: &protocol.DynamicTemplateMapping{
									Type: consts.MappingFieldTypeKeyword,
								},
							}},
							{"long_as_integer": &protocol.DynamicTemplate{
								MatchMappingType: consts.JSONFieldTypeLong,
								Mapping: &protocol.DynamicTemplateMapping{
									Type: consts.MappingFieldTypeInteger,
								},
							}},
							{"double_as_double": &protocol.DynamicTemplate{
								MatchMappingType: consts.JSONFieldTypeDouble,
								Mapping: &protocol.DynamicTemplateMapping{
									Type: consts.MappingFieldTypeDouble,
								},
							}},
						},
						Properties: map[string]*protocol.Property{
							consts.IDField:        {Type: consts.MappingFieldTypeKeyword},
							consts.TimestampField: {Type: consts.MappingFieldTypeDate},
						},
					},
				},
			},
			expectedTypes: map[string]string{
				consts.IDField:        consts.MappingFieldTypeKeyword,
				consts.TimestampField: consts.MappingFieldTypeDate,
				stringKey:             consts.MappingFieldTypeKeyword,
				dateKey:               consts.MappingFieldTypeKeyword,
				longKey:               consts.MappingFieldTypeInteger,
				doubleKey:             consts.MappingFieldTypeDouble,
			},
			result: true,
		},
		{
			name: "dynamic_template_match",
			docs: []protocol.Document{
				{
					consts.IDField:        "123456789",
					consts.TimestampField: "2023-02-22T19:47:36.499723+08:00",
					stringKey:             "string_value",
					stringKeyMatch:        "string_value",
					stringKeyUnmatch:      "string_value",
				},
			},
			index: &core.Index{
				Index: &protocol.Index{
					Name: "dynamic_template_match",
					Mappings: &protocol.Mappings{
						Dynamic: consts.DynamicMappingMode,
						DynamicTemplates: []map[string]*protocol.DynamicTemplate{
							{"string_as_keyword": &protocol.DynamicTemplate{
								MatchMappingType: consts.JSONFieldTypeString,
								MatchPattern:     utils.MatchModeRegex,
								Match:            "^string\\w[-\\w.+]*match$",
								Unmatch:          "^string\\w[-\\w.+]*unmatch$",
								Mapping: &protocol.DynamicTemplateMapping{
									Type: consts.MappingFieldTypeKeyword,
								},
							}},
						},
						Properties: map[string]*protocol.Property{
							consts.IDField:        {Type: consts.MappingFieldTypeKeyword},
							consts.TimestampField: {Type: consts.MappingFieldTypeDate},
						},
					},
				},
			},
			expectedTypes: map[string]string{
				consts.IDField:        consts.MappingFieldTypeKeyword,
				consts.TimestampField: consts.MappingFieldTypeDate,
				stringKey:             consts.MappingFieldTypeText,
				stringKeyMatch:        consts.MappingFieldTypeKeyword,
				stringKeyUnmatch:      consts.MappingFieldTypeText,
			},
			result: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testErr := core.BuildDocuments(tt.index, tt.docs)
			if tt.result {
				assert.NoError(t, testErr)
				for field, p := range tt.index.Mappings.Properties {
					assert.Equal(t, tt.expectedTypes[field], p.Type)
				}
			} else {
				assert.NotNil(t, testErr)
			}
		})
	}
}
