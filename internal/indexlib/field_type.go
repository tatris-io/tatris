// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

import (
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/utils"
)

// LibFieldType defines the field types supported by Tatris based on the underlying index library.
type LibFieldType struct {
	Type         string               // lib type
	MappingTypes []string             // Tatris mapping types
	Validator    func(value any) bool // used to validate whether the value matches the field type
}

// libFieldTypes maps from the index library type to mapping types supported by it.
var libFieldTypes = []LibFieldType{
	{
		Type: consts.LibFieldTypeNumeric,
		MappingTypes: []string{
			consts.MappingFieldTypeInteger,
			consts.MappingFieldTypeLong,
			consts.MappingFieldTypeFloat,
			consts.MappingFieldTypeDouble,
			consts.MappingFieldTypeShort,
			consts.MappingFieldTypeByte,
		},
		Validator: utils.IsNumeric,
	},
	{
		Type: consts.LibFieldTypeKeyword,
		MappingTypes: []string{
			consts.MappingFieldTypeKeyword,
			consts.MappingFieldTypeConstantKeyword,
		},
		Validator: utils.IsString,
	},
	{
		Type:         consts.LibFieldTypeText,
		MappingTypes: []string{consts.MappingFieldTypeText, consts.MappingFieldTypeMatchOnlyText},
		Validator:    utils.IsString,
	},
	{
		Type:         consts.LibFieldTypeBool,
		MappingTypes: []string{consts.MappingFieldTypeBool, consts.MappingFieldTypeBoolean},
		Validator:    utils.IsString,
	},
	{
		Type:         consts.LibFieldTypeDate,
		MappingTypes: []string{consts.MappingFieldTypeDate},
		Validator:    utils.IsDateType,
	},
}

// FieldTypes maps from the mapping type to the index library type it belongs to.
var FieldTypes map[string]LibFieldType

func init() {
	FieldTypes = make(map[string]LibFieldType)
	for _, lType := range libFieldTypes {
		for _, tType := range lType.MappingTypes {
			FieldTypes[tType] = lType
		}
	}
}

func ValidateMappingType(mType string) (bool, LibFieldType) {
	lType, found := FieldTypes[mType]
	return found, lType
}

func ValidateValue(mType string, value any) bool {
	if lType, found := FieldTypes[mType]; found {
		return lType.Validator(value)
	}
	return false
}

// DeduceType deduced Tatris field type from the value
// reference from:
// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/dynamic-field-mapping.html
func DeduceType(value any) (string, bool) {
	switch value := value.(type) {
	case string:
		if utils.IsDateType(value) {
			return consts.MappingFieldTypeDate, true
		}
		return consts.MappingFieldTypeText, true
	case bool:
		return consts.MappingFieldTypeBoolean, true
	case int64, int32, int16, int8, int, byte:
		return consts.MappingFieldTypeLong, true
	case float64, float32:
		return consts.MappingFieldTypeFloat, true
	default:
		return "", false
	}
}
