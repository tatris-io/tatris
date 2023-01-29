// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package metadata is about the management of metadata (i.e. index)
package metadata

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"

	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/common/log/logger"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/meta/metadata/storage"
	"github.com/tatris-io/tatris/internal/meta/metadata/storage/boltdb"
	"github.com/tatris-io/tatris/internal/protocol"
)

var metaStore storage.MetaStore
var metaCache *cache.Cache

var supportTypes = []string{
	"integer",
	"long",
	"float",
	"double",
	"boolean",
	"date",
	"keyword",
	"text",
}

func init() {
	var err error
	metaStore, err = boltdb.Open()
	if err != nil {
		panic("init metastore fail: " + err.Error())
	}
	metaCache = cache.New(5*time.Minute, 10*time.Minute)
}

func CreateIndex(index *core.Index) error {
	err := checkParam(index.Index)
	buildIndex(index)
	if err != nil {
		return err
	}
	logger.Info("create index", zap.Any("index", index))
	return SaveIndex(index)
}

func SaveIndex(index *core.Index) error {
	json, err := json.Marshal(index)
	if err != nil {
		return err
	}
	metaCache.Set(index.Name, index, 0)
	return metaStore.Set(fillKey(index.Name), json)
}

func GetIndex(indexName string) (*core.Index, error) {
	var index *core.Index
	cachedIndex, found := metaCache.Get(indexName)
	if found {
		index = cachedIndex.(*core.Index)
		return index, nil
	}
	// load
	if b, err := metaStore.Get(fillKey(indexName)); err != nil {
		return nil, err
	} else if b == nil {
		return nil, nil
	} else {
		index := &core.Index{}
		if err := json.Unmarshal(b, index); err != nil {
			return nil, err
		}
		shards := index.Shards
		if len(shards) > 0 {
			for _, shard := range shards {
				shard.Index = index
				segments := shard.Segments
				if len(segments) > 0 {
					for _, segment := range segments {
						segment.Shard = shard
					}
				}
			}
		}
		return index, nil
	}
}

func DeleteIndex(indexName string) error {
	metaCache.Delete(indexName)
	return metaStore.Delete(fillKey(indexName))
}

func buildIndex(index *core.Index) {
	numberOfShards := index.Settings.NumberOfShards
	shards := make([]*core.Shard, numberOfShards)
	for i := 0; i < numberOfShards; i++ {
		shards[i] = &core.Shard{}
		shards[i].ShardID = i
		shards[i].Index = index
	}
	index.Shards = shards
}

func checkParam(index *protocol.Index) error {
	mappings := index.Mappings
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
	err := checkReservedField(properties)
	if err != nil {
		return err
	}
	for _, property := range properties {
		err = checkType(property.Type)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkReservedField(properties map[string]protocol.Property) error {
	_, exist := properties[consts.IDField]
	if exist {
		return errors.New("_id is a built-in field")
	}
	properties[consts.IDField] = protocol.Property{Type: "keyword"}
	_, exist = properties[consts.TimestampField]
	if exist {
		return errors.New("_timestamp is a built-in field")
	}
	properties[consts.TimestampField] = protocol.Property{Type: "date"}
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

func fillKey(name string) string {
	return "/index/" + name
}
