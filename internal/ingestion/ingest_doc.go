// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package ingestion organizes codes of the ingestion routine
package ingestion

import (
	"time"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/common/utils"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/protocol"

	"github.com/tatris-io/tatris/internal/core/wal"
)

func IngestDocs(index *core.Index, docs []protocol.Document) error {
	shard := index.GetShardByRouting()
	if shard == nil {
		return &errs.NoShardError{Index: index.Name}
	}
	if err := buildDocuments(index, docs); err != nil {
		return err
	}
	return wal.ProduceWAL(shard, docs)
}

func buildDocuments(
	index *core.Index,
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
		err := core.CheckDocument(index, doc)
		if err != nil {
			return err
		}
	}
	return nil
}
