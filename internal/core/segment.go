// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package core

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/tatris-io/tatris/internal/core/config"

	"github.com/tatris-io/tatris/internal/common/log/logger"
	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/manage"
)

// Segment is a physical split of the index under a shard

const (
	// SegmentStatusWritable means segment is the latest segment of shard and is writable.
	// GetReaders returns a reader from the underlying writer, which can be used to search data near
	// real-time.
	SegmentStatusWritable uint8 = iota
	// SegmentStatusReadonly means segment is readonly. So the writer is always nil, and GetWriter
	// returns an error.
	// GetReader uses ReaderCache to ensure opening same only once.
	SegmentStatusReadonly
)

type Segment struct {
	Shard         *Shard `json:"-"`
	SegmentID     int
	Stat          SegmentStat
	SegmentStatus uint8
	lock          sync.Mutex
	writer        indexlib.Writer
	readerRef     int
}

func (segment *Segment) Status() uint8 {
	segment.lock.Lock()
	defer segment.lock.Unlock()

	return segment.SegmentStatus
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
	segment.lock.Lock()
	defer segment.lock.Unlock()

	if segment.Readonly() {
		return nil, errs.ErrSegmentReadonly
	}

	if reflect.ValueOf(segment.writer).IsValid() {
		return segment.writer, nil
	}

	return segment.openWriter()
}

// openWriter open underlying writer
func (segment *Segment) openWriter() (indexlib.Writer, error) {
	// open a writer
	config := indexlib.BuildConf(config.Cfg.Directory)
	writer, err := manage.GetWriter(
		config,
		*segment.Shard.Index.Mappings,
		segment.Shard.Index.GetName(),
		segment.GetName(),
	)
	if err != nil {
		return nil, err
	}
	segment.writer = writer
	return writer, nil
}

func (segment *Segment) openReaderFromWriter() (indexlib.Reader, error) {
	if !reflect.ValueOf(segment.writer).IsValid() {
		return nil, errors.New("writer is nil")
	}
	reader, err := segment.writer.Reader()
	if err != nil {
		return nil, err
	}
	segment.readerRef++
	wrap := &indexlib.HookReader{
		Reader: reader,
		CloseHook: func(reader indexlib.Reader) {
			reader.Close()
			segment.onReaderClose()
		},
	}
	return wrap, nil
}

func (segment *Segment) onReaderClose() {
	segment.lock.Lock()
	defer segment.lock.Unlock()

	segment.readerRef--
	if segment.SegmentStatus == SegmentStatusReadonly && segment.readerRef == 0 {
		segment.closeWriter()
	}
}

// GetReader returns a reader snapshot of current segment. So docs wrote this func returns are
// invisible to returned reader.
// Returned reader must be closed after use.
func (segment *Segment) GetReader() (indexlib.Reader, error) {
	segment.lock.Lock()
	defer segment.lock.Unlock()

	if segment.SegmentStatus == SegmentStatusWritable && reflect.ValueOf(segment.writer).IsValid() {
		return segment.openReaderFromWriter()
	}

	// The segment is readonly, so we can cache the result and reuse it
	if segment.SegmentStatus == SegmentStatusReadonly {
		config := indexlib.BuildConf(config.Cfg.Directory)
		return manage.GetReaderUsingCache(config, segment.GetName())
	}

	// The segment is never write since server startup. So we force open the writer here.
	_, err := segment.openWriter()
	if err != nil {
		return nil, err
	}
	return segment.openReaderFromWriter()
}

func (segment *Segment) IsMature() bool {
	return segment.Stat.DocNum > config.Cfg.Segment.MatureThreshold ||
		segment.SegmentStatus == SegmentStatusReadonly
}

func (segment *Segment) Readonly() bool {
	return segment.SegmentStatus != SegmentStatusWritable
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

// OnMature is called when segment becomes mature.
// It marks segment readonly and closes the underlying writer.
func (segment *Segment) OnMature() {
	segment.lock.Lock()
	defer segment.lock.Unlock()

	segment.SegmentStatus = SegmentStatusReadonly

	// close only when readerRef is 0
	if reflect.ValueOf(segment.writer).IsValid() && segment.readerRef == 0 {
		segment.closeWriter()
	}

	segment.Stat.MatureTime = time.Now().UnixMilli()

	logger.Info(
		"segment is mature",
		zap.String("segment", segment.GetName()),
		zap.Int64("docNum", segment.Stat.DocNum),
		zap.Int64("timeRange", segment.Stat.MaxTime-segment.Stat.MinTime),
		zap.Int64("duration", segment.Stat.MatureTime-segment.Stat.CreateTime),
	)
}

func (segment *Segment) closeWriter() {
	segment.writer.Close()
	segment.writer = nil
}

func (segment *Segment) Destroy() {
	// set the status to SegmentStatusReadonly,
	// so immature segment can also close its writer after the last reader is closed
	segment.SegmentStatus = SegmentStatusReadonly
}
