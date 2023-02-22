// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

import (
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/utils"
)

type LibFieldType struct {
	Type         string
	MappingTypes []string
	Validator    func(value any) bool
}

// libFieldTypes maps from the index library type to mapping types supported by it
var libFieldTypes = []LibFieldType{
	{
		Type: consts.LibFieldTypeNumeric,
		MappingTypes: []string{
			consts.Integer,
			consts.Long,
			consts.Float,
			consts.Double,
			consts.Short,
			consts.Byte,
		},
		Validator: utils.IsNumeric,
	},
	{
		Type:         consts.LibFieldTypeKeyword,
		MappingTypes: []string{consts.Keyword, consts.ConstantKeyword},
		Validator:    utils.IsString,
	},
	{
		Type:         consts.LibFieldTypeText,
		MappingTypes: []string{consts.Text, consts.MatchOnlyText},
		Validator:    utils.IsString,
	},
	{
		Type:         consts.LibFieldTypeBool,
		MappingTypes: []string{consts.Bool, consts.Boolean},
		Validator:    utils.IsString,
	},
	{
		Type:         consts.LibFieldTypeDate,
		MappingTypes: []string{consts.Date},
		Validator:    utils.IsDateType,
	},
}

// FieldTypes maps from the mapping type to the index library type it belongs to
var FieldTypes map[string]LibFieldType

func init() {
	FieldTypes = make(map[string]LibFieldType)
	for _, lType := range libFieldTypes {
		for _, tType := range lType.MappingTypes {
			FieldTypes[tType] = lType
		}
	}
}

func ValidateMappingType(t string) (bool, LibFieldType) {
	lType, found := FieldTypes[t]
	return found, lType
}

func ValidateValue(t string, v any) bool {
	if lType, found := FieldTypes[t]; found {
		return lType.Validator(v)
	}
	return false
}

func DeduceType(value any) string {
	switch value := value.(type) {
	case string:
		if utils.IsDateType(value) {
			return consts.Date
		}
		return consts.Text
	case bool:
		return consts.Boolean
	case int64, int32, int16, int8, int, byte:
		return consts.Long
	case float64, float32:
		return consts.Double
	default:
		return ""
	}
}
