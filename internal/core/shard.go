// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package core

import (
	"sync"
)

// Shard is a logical split of the index
type Shard struct {
	Index    *Index `json:"-"`
	ShardID  int
	Segments []*Segment
	lock     sync.RWMutex
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
			shard.addSegment(shard.GetSegmentNum())
		}
	}
}

func (shard *Shard) addSegment(segmentID int) {
	segments := shard.Segments
	if len(segments) == 0 {
		segments = make([]*Segment, 0)
		shard.Segments = segments
	}
	shard.Segments = append(segments, &Segment{Shard: shard, SegmentID: segmentID, Stat: Stat{}})
}
