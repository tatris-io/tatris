// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package consts

import "strings"

// field types supported by Tatris
const (
	MappingFieldTypeInteger         = "integer"
	MappingFieldTypeLong            = "long"
	MappingFieldTypeFloat           = "float"
	MappingFieldTypeDouble          = "double"
	MappingFieldTypeShort           = "short"
	MappingFieldTypeByte            = "byte"
	MappingFieldTypeKeyword         = "keyword"
	MappingFieldTypeConstantKeyword = "constant_keyword"
	MappingFieldTypeBool            = "bool"
	MappingFieldTypeBoolean         = "boolean"
	MappingFieldTypeText            = "text"
	MappingFieldTypeMatchOnlyText   = "match_only_text"
	MappingFieldTypeDate            = "date"
)

// field types supported by the underlying index library
const (
	LibFieldTypeNumeric = "numeric"
	LibFieldTypeKeyword = "keyword"
	LibFieldTypeBool    = "bool"
	LibFieldTypeText    = "text"
	LibFieldTypeDate    = "date"
)

// JSON field types
const (
	JSONFieldTypeString  = "string"
	JSONFieldTypeLong    = "long"
	JSONFieldTypeDouble  = "double"
	JSONFieldTypeBoolean = "boolean"
	JSONFieldTypeDate    = "date"
	// JSONFieldTypeObject  = "object"
	// JSONFieldTypeBinary  = "binary"
)

// dynamic modes supported by Tatris
const (
	StrictMappingMode  = "strict"
	IgnoreMappingMode  = "false"
	DynamicMappingMode = "true"
)

func IsJSONFieldType(t string) bool {
	return strings.EqualFold(t, JSONFieldTypeString) || strings.EqualFold(t, JSONFieldTypeLong) ||
		strings.EqualFold(t, JSONFieldTypeDouble) ||
		strings.EqualFold(t, JSONFieldTypeBoolean) ||
		strings.EqualFold(t, JSONFieldTypeDate)
}
