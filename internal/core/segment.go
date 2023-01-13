// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package core

import (
	"fmt"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/manage"
	"sync"
	"time"
)

// Segment is a physical split of the index under a shard
type Segment struct {
	Shard     *Shard `json:"-"`
	SegmentID int
	Stat      Stat
	lock      sync.RWMutex
	writer    indexlib.Writer
}

func (segment *Segment) GetWriter() (indexlib.Writer, error) {
	if segment.writer != nil {
		return segment.writer, nil
	}
	indexName := segment.Shard.Index.Name
	shardID := segment.Shard.ShardID
	// open a writer
	config := &indexlib.BaseConfig{
		Index:    fmt.Sprintf("%s/%d/%d", indexName, shardID, segment.SegmentID),
		DataPath: consts.DefaultDataPath,
	}
	writer, err := manage.GetWriter(config)
	if err != nil {
		return nil, err
	}
	segment.writer = writer
	return writer, nil
}

func (segment *Segment) GetReader() (indexlib.Reader, error) {
	indexName := segment.Shard.Index.Name
	shardID := segment.Shard.ShardID
	segmentID := segment.SegmentID
	config := &indexlib.BaseConfig{
		Index:    fmt.Sprintf("%s/%d/%d", indexName, shardID, segmentID),
		DataPath: consts.DefaultDataPath,
	}
	return manage.GetReader(config)
}

func (segment *Segment) IsMature() bool {
	// TODO to be configurable
	return segment.Stat.DocNum > 5
}

func (segment *Segment) MatchTime(start, end int64) bool {
	return start <= segment.Stat.MaxTime && end >= segment.Stat.MinTime
}

func (segment *Segment) UpdateStat(timestamp time.Time) {
	t := timestamp.UnixMilli()
	segment.lock.Lock()
	defer segment.lock.Unlock()
	segment.Stat.DocNum++
	if segment.Stat.MinTime == 0 || segment.Stat.MaxTime == 0 {
		segment.Stat.MinTime = t
		segment.Stat.MaxTime = t
	} else if segment.Stat.MinTime > t {
		segment.Stat.MinTime = t
	} else if segment.Stat.MaxTime < t {
		segment.Stat.MaxTime = t
	}
}
