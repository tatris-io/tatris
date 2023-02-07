// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package consts

const (
	NumericMappingType = "numeric"
	KeywordMappingType = "keyword"
	BoolMappingType    = "bool"
	TextMappingType    = "text"
	DateMappingType    = "date"

	StrictMappingMode  = "strict"
	IgnoreMappingMode  = "false"
	DynamicMappingMode = "true"
)

var MappingTypes map[string]string

var numberType = []string{"integer", "long", "float", "double", "short", "byte"}
var keywordType = []string{"keyword", "constant_keyword"}
var booleanType = []string{"boolean", "bool"}
var textType = []string{"text", "match_only_text"}
var dateType = []string{"date"}

func init() {
	MappingTypes = make(map[string]string)
	for _, t := range numberType {
		MappingTypes[t] = NumericMappingType
	}
	for _, t := range keywordType {
		MappingTypes[t] = KeywordMappingType
	}
	for _, t := range booleanType {
		MappingTypes[t] = BoolMappingType
	}
	for _, t := range textType {
		MappingTypes[t] = TextMappingType
	}
	for _, t := range dateType {
		MappingTypes[t] = DateMappingType
	}
}
