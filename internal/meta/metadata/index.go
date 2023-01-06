// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package metadata is about the management of metadata (i.e. index)
package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tatris-io/tatris/internal/meta/metadata/storage"
	"github.com/tatris-io/tatris/internal/meta/metadata/storage/boltdb"
	"github.com/tatris-io/tatris/internal/protocol"
	"strings"
)

var metaStore storage.MetaStore
var supportTypes = []string{"integer", "long", "float", "double", "boolean", "date", "keyword", "text"}

func init() {
	metaStore, _ = boltdb.Open()
}

func Create(idx *protocol.Index) error {
	json, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	err = checkParam(idx)
	if err != nil {
		return err
	}
	return metaStore.Set(fillKey(idx.Name), json)
}

func checkParam(idx *protocol.Index) error {
	mappings := idx.Mappings
	if mappings == nil {
		return errors.New("mappings can not be empty")
	}
	err := checkMapping(mappings)
	if err != nil {
		return err
	}
	return nil
}

func checkMapping(mappings *protocol.Mappings) error {
	properties := mappings.Properties
	if properties == nil {
		return errors.New("mappings.properties can not be empty")
	}
	for _, property := range properties {
		err := checkType(property.Type)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkType(paramType string) error {
	for _, supportType := range supportTypes {
		if strings.EqualFold(paramType, supportType) {
			return nil
		}
	}
	return fmt.Errorf("the type %s is not supported", paramType)
}

func Get(idxName string) (*protocol.Index, error) {
	if b, err := metaStore.Get(fillKey(idxName)); err != nil {
		return nil, err
	} else if b == nil {
		return nil, nil
	} else {
		idx := new(protocol.Index)
		if err := json.Unmarshal(b, idx); err != nil {
			return nil, err
		}
		return idx, nil
	}
}

func fillKey(name string) string {
	return "/index/" + name
}
