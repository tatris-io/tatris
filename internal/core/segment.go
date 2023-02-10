// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package core

import (
	"fmt"
	"sync"
	"time"

	"github.com/tatris-io/tatris/internal/core/config"

	"github.com/tatris-io/tatris/internal/common/log/logger"
	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/manage"
)

// Segment is a physical split of the index under a shard
type Segment struct {
	Shard     *Shard `json:"-"`
	SegmentID int
	Stat      Stat
	lock      sync.RWMutex
	writer    indexlib.Writer
}

func (segment *Segment) GetName() string {
	return fmt.Sprintf(
		"%s/%d/%d",
		segment.Shard.Index.Name,
		segment.Shard.ShardID,
		segment.SegmentID,
	)
}

func (segment *Segment) GetWriter() (indexlib.Writer, error) {
	if segment.writer != nil {
		return segment.writer, nil
	}
	// open a writer
	config := &indexlib.BaseConfig{
		DataPath: consts.DefaultDataPath,
	}
	writer, err := manage.GetWriter(config, segment.Shard.Index.Mappings, segment.GetName())
	if err != nil {
		return nil, err
	}
	segment.writer = writer
	return writer, nil
}

func (segment *Segment) GetReader() (indexlib.Reader, error) {
	config := &indexlib.BaseConfig{
		DataPath: consts.DefaultDataPath,
	}
	return manage.GetReader(config, segment.Shard.Index.Mappings, segment.GetName())
}

func (segment *Segment) IsMature() bool {
	return segment.Stat.DocNum > config.Cfg.Segment.MatureThreshold
}

func (segment *Segment) MatchTime(start, end int64) bool {
	return start <= segment.Stat.MaxTime && end >= segment.Stat.MinTime
}

func (segment *Segment) UpdateStat(min, max time.Time, docs int64) {
	mint := min.UnixMilli()
	maxt := max.UnixMilli()
	segment.lock.Lock()
	defer segment.lock.Unlock()
	if segment.Stat.MinTime == 0 {
		segment.Stat.MinTime = mint
	}
	if segment.Stat.MaxTime == 0 {
		segment.Stat.MaxTime = maxt
	}

	if mint != 0 && segment.Stat.MinTime > mint {
		segment.Stat.MinTime = mint
	}
	if maxt != 0 && segment.Stat.MaxTime < maxt {
		segment.Stat.MaxTime = maxt
	}
	segment.Stat.DocNum += docs
	logger.Info(
		"update segment stat",
		zap.Int64("minTime", segment.Stat.MinTime),
		zap.Int64("maxTime", segment.Stat.MaxTime),
		zap.Int64("docNum", segment.Stat.DocNum),
	)
}
