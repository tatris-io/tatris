// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package consts

import (
	"strings"

	mapset "github.com/deckarep/golang-set/v2"
)

const (
	ByteMappingType    = "byte"
	BooleanMappingType = "boolean"
	UnknownMappingType = "unknown"

	DynamicMappingConfig  = "true"
	ExplicitMappingConfig = "false"
	StrictMappingConfig   = "strict"
)

var ValidDynamicMappingTypes map[string]mapset.Set[string]
var ValidFieldTypes map[string]mapset.Set[string]

var floatTypes = []string{"int", "int8", "int16", "int32", "float32"}
var doubleTypes = []string{"int", "int8", "int16", "int32", "float32", "int64", "float64"}
var intTypes = []string{"int", "int8", "int16", "int32"}
var shortTypes = []string{"int"}
var longTypes = []string{"int", "int8", "int16", "int32", "int64"}
var stringTypes = []string{"string"}

func init() {
	ValidDynamicMappingTypes = make(map[string]mapset.Set[string])
	var stringMappingTypes []string
	stringMappingTypes = append(stringMappingTypes, keywordType...)
	stringMappingTypes = append(stringMappingTypes, textType...)
	stringMappingTypes = append(stringMappingTypes, dateType...)
	for _, t := range stringMappingTypes {
		if strings.EqualFold(t, DateMappingType) {
			continue
		}
		ValidDynamicMappingTypes[t] = mapset.NewSet[string]()
		for _, tt := range stringMappingTypes {
			ValidDynamicMappingTypes[t].Add(tt)
		}
	}
	ValidDynamicMappingTypes[DateMappingType] = mapset.NewSet(DateMappingType)

	for _, t := range numberType {
		if strings.EqualFold(t, ByteMappingType) {
			continue
		}
		ValidDynamicMappingTypes[t] = mapset.NewSet[string]()
		for _, tt := range numberType {
			ValidDynamicMappingTypes[t].Add(tt)
		}
	}
	ValidDynamicMappingTypes[ByteMappingType] = mapset.NewSet(ByteMappingType)
	ValidDynamicMappingTypes[BooleanMappingType] = mapset.NewSet(BooleanMappingType)

	ValidFieldTypes = make(map[string]mapset.Set[string])

	for _, typeName := range numberType {
		switch typeName {
		case "integer":
			ValidFieldTypes[typeName] = mapset.NewSet(intTypes...)
		case "long":
			ValidFieldTypes[typeName] = mapset.NewSet(longTypes...)
		case "float":
			ValidFieldTypes[typeName] = mapset.NewSet(floatTypes...)
		case "double":
			ValidFieldTypes[typeName] = mapset.NewSet(doubleTypes...)
		case "short":
			ValidFieldTypes[typeName] = mapset.NewSet(shortTypes...)
		case "byte":
			ValidFieldTypes[typeName] = mapset.NewSet(shortTypes...)
		}
	}

	for _, typeName := range keywordType {
		ValidFieldTypes[typeName] = mapset.NewSet(stringTypes...)
	}

	for _, typeName := range textType {
		ValidFieldTypes[typeName] = mapset.NewSet(stringTypes...)
	}

	for _, typeName := range dateType {
		ValidFieldTypes[typeName] = mapset.NewSet(stringTypes...)
	}

	for _, typeName := range booleanType {
		ValidFieldTypes[typeName] = mapset.NewSet(BoolMappingType)
	}
}
