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
			docTimestamp = timestamp.(time.Time)
		}
		doc[consts.IDField] = docID
		doc[consts.TimestampField] = docTimestamp
		err := CheckDocument(index, doc)
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

	properties := index.Mappings.Properties
	iDynamic := index.Mappings.Dynamic

	for k, v := range doc {
		// get field-level dynamic mode
		fDynamic := fieldLevelDynamic(iDynamic, properties, k)
		// get field type, possibly deduced by dynamic mode and value if not explicitly defined
		fType, err := fieldType(fDynamic, properties, k, v)
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
			p := &protocol.Property{
				Type:    fType,
				Dynamic: consts.DynamicMappingMode,
			}
			properties[k] = p
		}
	}
	index.Mappings.Properties = properties
	return nil
}

func fieldType(
	dynamic string, properties map[string]*protocol.Property, field string, value interface{},
) (string, error) {
	// if the field has been explicitly defined, return
	if property, ok := properties[field]; ok && property.Type != "" {
		return property.Type, nil
	}
	// otherwise, try to deduce the field type
	return deduceFieldType(dynamic, field, value)
}

func deduceFieldType(dynamic string, field string, value interface{}) (string, error) {
	switch dynamic {
	case consts.DynamicMappingMode:
		if t := indexlib.DeduceType(value); t != "" {
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
