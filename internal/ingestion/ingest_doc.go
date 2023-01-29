// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package ingestion organizes codes of the ingestion routine
package ingestion

import (
	"errors"
	"fmt"
	"github.com/tatris-io/tatris/internal/core"
	"time"

	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/common/log/logger"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/utils"
	"github.com/tatris-io/tatris/internal/meta/metadata"
)

func IngestDocs(indexName string, docs []map[string]interface{}) error {
	index, err := metadata.GetIndex(indexName)
	if err != nil {
		return err
	}
	idDocs, err := buildDocs(index, docs)
	if err != nil {
		return errors.New(fmt.Sprintf("fail to check mapping for %s", err.Error()))
	}
	shard := index.GetShardByRouting()
	if shard == nil {
		return errors.New("shard not found")
	}
	shard.CheckSegments()
	segment := shard.GetLatestSegment()
	if segment == nil {
		return errors.New("segment not found")
	}
	writer, err := segment.GetWriter()
	if err != nil {
		return err
	}
	logger.Info(
		"ready to ingest docs",
		zap.String("index", shard.Index.Name),
		zap.Int("shard", shard.ShardID),
		zap.Int("size", len(idDocs)),
	)
	for docID, doc := range idDocs {
		err := writer.Insert(docID, doc, index.Mappings)
		if err != nil {
			return err
		}
		timestamp := doc[consts.TimestampField]
		segment.UpdateStat(timestamp.(time.Time))
	}
	return metadata.SaveIndex(index)
}

func buildDocs(index *core.Index, docs []map[string]interface{}) (map[string]map[string]interface{}, error) {
	idDocs := make(map[string]map[string]interface{})
	for _, doc := range docs {
		docID := ""
		docTimestamp := time.Now()
		if id, ok := doc[consts.IDField]; ok && id != nil && id != "" {
			docID = id.(string)
		} else {
			docID = utils.GenerateID()
		}
		if timestamp, ok := doc[consts.TimestampField]; ok && timestamp != nil {
			docTimestamp = timestamp.(time.Time)
		}
		doc[consts.IDField] = docID
		doc[consts.TimestampField] = docTimestamp
		err := index.CheckMapping(docID, doc)
		if err != nil {
			return idDocs, err
		}
		idDocs[docID] = doc
	}
	return idDocs, nil
}
