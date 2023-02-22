// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package consts

// field types supported by Tatris
const (
	Integer         = "integer"
	Long            = "long"
	Float           = "float"
	Double          = "double"
	Short           = "short"
	Byte            = "byte"
	Keyword         = "keyword"
	ConstantKeyword = "constant_keyword"
	Bool            = "bool"
	Boolean         = "boolean"
	Text            = "text"
	MatchOnlyText   = "match_only_text"
	Date            = "date"
)

// field types supported by the underlying index library
const (
	LibFieldTypeNumeric = "numeric"
	LibFieldTypeKeyword = "keyword"
	LibFieldTypeBool    = "bool"
	LibFieldTypeText    = "text"
	LibFieldTypeDate    = "date"
)

// dynamic modes supported by Tatris
const (
	StrictMappingMode  = "strict"
	IgnoreMappingMode  = "false"
	DynamicMappingMode = "true"
)
