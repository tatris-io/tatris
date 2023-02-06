// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package core

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/common/log/logger"
)

// Shard is a logical split of the index
type Shard struct {
	Index    *Index `json:"-"`
	ShardID  int
	Segments []*Segment
	lock     sync.RWMutex
	Stat     ShardStat
}

func (shard *Shard) GetName() string {
	return fmt.Sprintf("%s/%d", shard.Index.Name, shard.ShardID)
}

func (shard *Shard) GetSegmentNum() int {
	return len(shard.Segments)
}

func (shard *Shard) GetSegments() []*Segment {
	return shard.Segments
}

func (shard *Shard) GetSegment(idx int) *Segment {
	return shard.Segments[idx]
}

func (shard *Shard) GetLatestSegment() *Segment {
	num := shard.GetSegmentNum()
	if num == 0 {
		return nil
	}
	return shard.Segments[num-1]
}

func (shard *Shard) CheckSegments() {
	lastedSegment := shard.GetLatestSegment()
	if lastedSegment == nil || lastedSegment.IsMature() {
		shard.lock.Lock()
		defer shard.lock.Unlock()
		lastedSegment = shard.GetLatestSegment()
		if lastedSegment == nil || lastedSegment.IsMature() {
			newID := shard.GetSegmentNum()
			shard.addSegment(newID)
			logger.Info(
				"add segment",
				zap.String("index", shard.Index.Name),
				zap.Int("shard", shard.ShardID),
				zap.Int("segment", newID),
			)
		}
	}
}

func (shard *Shard) UpdateStat(min, max time.Time, docs int64, wals uint64) {
	mint := min.UnixMilli()
	maxt := max.UnixMilli()
	shard.lock.Lock()
	defer shard.lock.Unlock()
	if shard.Stat.MinTime == 0 {
		shard.Stat.MinTime = mint
	}
	if shard.Stat.MaxTime == 0 {
		shard.Stat.MaxTime = maxt
	}

	if mint != 0 && shard.Stat.MinTime > mint {
		shard.Stat.MinTime = mint
	}
	if maxt != 0 && shard.Stat.MaxTime < maxt {
		shard.Stat.MaxTime = maxt
	}
	shard.Stat.DocNum += docs
	if wals != 0 {
		shard.Stat.WalIndex = wals
	}
	logger.Info(
		"update shard stat",
		zap.Int64("minTime", shard.Stat.MinTime),
		zap.Int64("maxTime", shard.Stat.MaxTime),
		zap.Int64("docNum", shard.Stat.DocNum),
		zap.Uint64("walIndex", shard.Stat.WalIndex),
	)
}

func (shard *Shard) addSegment(segmentID int) {
	segments := shard.Segments
	if len(segments) == 0 {
		segments = make([]*Segment, 0)
		shard.Segments = segments
	}
	shard.Segments = append(segments, &Segment{Shard: shard, SegmentID: segmentID, Stat: Stat{}})
}
