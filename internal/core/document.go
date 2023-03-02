// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package core

import (
	"strings"
	"time"

	"github.com/tatris-io/tatris/internal/common/utils"

	"github.com/tatris-io/tatris/internal/indexlib"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/protocol"
)

func BuildDocuments(
	index *Index,
	docs []protocol.Document,
) error {
	for _, doc := range docs {
		var err error
		docID := ""
		docTimestamp := time.Now()
		if id, ok := doc[consts.IDField]; ok && id != nil && id != "" {
			docID = id.(string)
		} else {
			genID, err := utils.GenerateID()
			if err != nil {
				return err
			}
			docID = genID
		}
		if timestamp, ok := doc[consts.TimestampField]; ok && timestamp != nil {
			docTimestamp, err = utils.ParseTime(timestamp)
			if err != nil {
				return err
			}
		}
		doc[consts.IDField] = docID
		doc[consts.TimestampField] = docTimestamp
		err = CheckDocument(index, doc)
		if err != nil {
			return err
		}
	}
	return nil
}

func CheckDocument(index *Index, doc protocol.Document) error {
	if index.Index == nil || index.Mappings == nil || index.Mappings.Properties == nil {
		return errs.ErrEmptyMappings
	}

	mappings := index.Mappings
	properties := mappings.Properties
	iDynamic := mappings.Dynamic
	dynamicTemplates := mappings.DynamicTemplates

	newProperties := make(map[string]*protocol.Property)

	for k, v := range doc {
		// get field-level dynamic mode
		fDynamic := fieldLevelDynamic(iDynamic, properties, k)
		// get field type, possibly deduced by dynamic mode and value if not explicitly defined
		fType, err := fieldType(fDynamic, dynamicTemplates, properties, k, v)
		if err != nil {
			return err
		}
		if fType == "" {
			continue
		}
		// check if field type and value are compatible
		err = checkValue(fType, k, v)
		if err != nil {
			return err
		}
		// if new valid field types have been deduced, store them into the index metadata
		if _, ok := properties[k]; !ok && strings.EqualFold(iDynamic, consts.DynamicMappingMode) {
			newProperties[k] = &protocol.Property{
				Type:    fType,
				Dynamic: consts.DynamicMappingMode,
			}
		}
	}
	index.AddProperties(newProperties)
	return nil
}

func fieldType(
	dynamic string,
	dynamicTemplates []map[string]*protocol.DynamicTemplate,
	properties map[string]*protocol.Property,
	field string,
	value interface{},
) (string, error) {
	// if the field has been explicitly defined, return
	if property, ok := properties[field]; ok && property.Type != "" {
		return property.Type, nil
	}
	// otherwise, try to get dynamic type
	return dynamicFieldType(dynamic, dynamicTemplates, field, value)
}

func dynamicFieldType(
	dynamic string,
	dynamicTemplates []map[string]*protocol.DynamicTemplate,
	field string,
	value interface{},
) (string, error) {
	switch dynamic {
	case consts.DynamicMappingMode:
		// if a dynamic template is matched, apply its specified type
		if t, matched := matchDynamicTemplate(dynamicTemplates, field, value); matched {
			return t, nil
		}
		// deduce from value
		if t, deduced := indexlib.DeduceType(value); deduced {
			return t, nil
		}
		return "", &errs.InvalidFieldValError{Field: field, Value: value}
	case consts.IgnoreMappingMode:
		return "", nil
	case consts.StrictMappingMode:
		return "", &errs.InvalidFieldValError{Field: field, Type: "_strict", Value: value}
	default:
		return "", &errs.UnsupportedError{Desc: "dynamic mode", Value: dynamic}
	}
}

func matchDynamicTemplate(
	dynamicTemplates []map[string]*protocol.DynamicTemplate,
	field string,
	value any,
) (string, bool) {
	for _, dynamicTemplate := range dynamicTemplates {
		for _, dt := range dynamicTemplate {
			if dt.Match != "" && !utils.Match(dt.Match, field, dt.MatchPattern) {
				continue
			}
			if dt.Unmatch != "" && utils.Match(dt.Unmatch, field, dt.MatchPattern) {
				continue
			}
			if dt.MatchMappingType != "" &&
				!(strings.EqualFold(dt.MatchMappingType, consts.JSONFieldTypeString) && utils.IsString(value) ||
					strings.EqualFold(dt.MatchMappingType, consts.JSONFieldTypeLong) && utils.IsInteger(value) ||
					strings.EqualFold(dt.MatchMappingType, consts.JSONFieldTypeDouble) && utils.IsFloat(value) ||
					strings.EqualFold(dt.MatchMappingType, consts.JSONFieldTypeBoolean) && utils.IsBool(value) ||
					strings.EqualFold(dt.MatchMappingType, consts.JSONFieldTypeDate) && utils.IsDateType(value)) {
				continue
			}
			return dt.Mapping.Type, true
		}
	}
	return "", false
}

func checkValue(t string, field string, value any) error {
	if indexlib.ValidateValue(t, value) {
		return nil
	}
	return &errs.InvalidFieldValError{Field: field, Type: t, Value: value}
}

func fieldLevelDynamic(
	dynamic string,
	properties map[string]*protocol.Property,
	fieldName string,
) string {
	if property, ok := properties[fieldName]; ok {
		if property.Dynamic != "" {
			return property.Dynamic
		}
	}
	return dynamic
}
