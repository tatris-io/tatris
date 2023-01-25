// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package core contains the implementation of Tatris core features
package core

import (
	"fmt"

	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/protocol"
	"go.uber.org/zap"
)

type Index struct {
	*protocol.Index
	Shards []*Shard `json:"shards"`
}

func (index *Index) GetShardNum() int {
	return len(index.Shards)
}

func (index *Index) GetShards() []*Shard {
	return index.Shards
}

func (index *Index) GetShard(idx int) *Shard {
	return index.Shards[idx]
}

// GetShardByRouting
// TODO: build the real route, temporarily think that there is always only 1 shard
func (index *Index) GetShardByRouting() *Shard {
	for _, shard := range index.Shards {
		return shard
	}
	return nil
}

func (index *Index) GetReadersByTime(start, end int64) ([]indexlib.Reader, error) {
	splits := make([]string, 0)
	readers := make([]indexlib.Reader, 0)
	for _, shard := range index.Shards {
		for _, segment := range shard.Segments {
			if segment.MatchTime(start, end) {
				reader, err := segment.GetReader()
				if err != nil {
					return nil, err
				}
				splits = append(splits, fmt.Sprintf("%d/%d", shard.ShardID, segment.SegmentID))
				readers = append(readers, reader)
			}
		}
	}
	logger.Info(
		"find readers",
		zap.String("index", index.Name),
		zap.Int64("start", start),
		zap.Int64("end", end),
		zap.Int("size", len(readers)),
		zap.Any("splits", splits),
	)
	return readers, nil
}
