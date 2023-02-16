// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package core

import (
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/core/config"
	"github.com/tatris-io/tatris/internal/core/wal/log"
	"github.com/tatris-io/tatris/internal/core/wal/tidwall"
	"github.com/tidwall/wal"

	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/common/log/logger"
)

// Shard is a logical split of the index
type Shard struct {
	Index    *Index `json:"-"`
	ShardID  int
	Segments []*Segment
	Stat     ShardStat
	wal      log.WalLog
	lock     sync.RWMutex
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

func (shard *Shard) GetLatestSegmentID() int {
	return shard.GetSegmentNum() - 1
}

func (shard *Shard) GetLatestSegment() *Segment {
	SegmentID := shard.GetLatestSegmentID()
	if SegmentID < 0 {
		return nil
	}
	return shard.Segments[SegmentID]
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
			if lastedSegment != nil {
				lastedSegment.onMature()
			}
			logger.Info(
				"add segment",
				zap.String("index", shard.Index.Name),
				zap.Int("shard", shard.ShardID),
				zap.Int("segment", newID),
			)
		}
	}
}

// ForceAddSegment forces adding a segment to current shard
func (shard *Shard) ForceAddSegment() {
	shard.lock.Lock()
	defer shard.lock.Unlock()

	lastedSegment := shard.GetLatestSegment()
	newID := shard.GetSegmentNum()
	shard.addSegment(newID)
	if lastedSegment != nil {
		lastedSegment.onMature()
	}
	logger.Info(
		"add segment",
		zap.String("index", shard.Index.Name),
		zap.Int("shard", shard.ShardID),
		zap.Int("segment", newID),
	)
}

func (shard *Shard) OpenWAL() (log.WalLog, error) {
	shard.lock.Lock()
	defer shard.lock.Unlock()
	if shard.wal == nil {
		options := config.Cfg.Wal
		name := shard.GetName()
		p := path.Join(consts.DefaultWALPath, name)
		logger.Info("open wal", zap.String("name", name), zap.Any("options", options))
		twalLog := &tidwall.TWalLog{}
		twalOptions := &wal.Options{}
		twalOptions.NoSync = options.NoSync
		twalOptions.SegmentSize = options.SegmentSize
		if options.LogFormat == 1 {
			twalOptions.LogFormat = wal.JSON
		} else {
			twalOptions.LogFormat = wal.Binary
		}
		twalOptions.SegmentCacheSize = options.SegmentCacheSize
		twalOptions.NoCopy = options.NoCopy
		twalOptions.DirPerms = options.DirPerms
		twalOptions.FilePerms = options.FilePerms
		l, err := wal.Open(p, twalOptions)
		if err != nil {
			return nil, err
		}
		twalLog.Log = l
		shard.wal = twalLog
	}
	return shard.wal, nil
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
	shard.Segments = append(
		shard.Segments,
		&Segment{Shard: shard, SegmentID: segmentID, Stat: Stat{}},
	)
}
