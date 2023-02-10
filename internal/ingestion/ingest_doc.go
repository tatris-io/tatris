// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package ingestion organizes codes of the ingestion routine
package ingestion

import (
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/protocol"

	"github.com/tatris-io/tatris/internal/core/wal"
)

func IngestDocs(index *core.Index, docs []protocol.Document) error {
	shard := index.GetShardByRouting()
	if shard == nil {
		return &errs.ShardNotFoundError{Index: index.Name, Shard: -1}
	}
	return wal.ProduceWAL(shard, docs)
}
