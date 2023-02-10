// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package ingestion organizes codes of the ingestion routine
package ingestion

import (
	"errors"
	"fmt"

	"github.com/tatris-io/tatris/internal/core/wal"
	"github.com/tatris-io/tatris/internal/meta/metadata"
)

func IngestDocs(indexName string, docs []map[string]interface{}) error {
	index, err := metadata.GetIndex(indexName)
	if err != nil {
		return err
	}
	if index == nil {
		return errors.New("index not found: " + indexName)
	}
	shard := index.GetShardByRouting()
	if shard == nil {
		return fmt.Errorf("shard not found, index=%s", indexName)
	}
	return wal.ProduceWAL(shard, docs)
}
