// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package ingestion organizes codes of the ingestion routine
package ingestion

import (
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/protocol"

	"github.com/tatris-io/tatris/internal/core/wal"
	"github.com/tatris-io/tatris/internal/meta/metadata"
)

func IngestDocs(indexName string, docs []protocol.Document) error {
	index, err := metadata.GetIndex(indexName)
	if err != nil {
		return err
	}
	if index == nil {
		return &errs.IndexNotFoundError{Index: indexName}
	}
	shard := index.GetShardByRouting()
	if shard == nil {
		return &errs.ShardNotFoundError{Index: indexName, Shard: -1}
	}
	return wal.ProduceWAL(shard, docs)
}
