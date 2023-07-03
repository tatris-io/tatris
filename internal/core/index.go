// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package core contains the implementation of Tatris core features
package core

import (
	"os"
	"path"
	"strings"
	"sync"

	"github.com/tatris-io/tatris/internal/indexlib/bluge/directory/oss"

	"github.com/tatris-io/tatris/internal/common/consts"

	"github.com/tatris-io/tatris/internal/core/config"

	"github.com/pkg/errors"
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/protocol"
	"go.uber.org/zap"
)

type Index struct {
	*protocol.Index
	Shards []*Shard `json:"shards"`
	lock   sync.RWMutex
}

func (index *Index) GetName() string {
	return index.Name
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

func (index *Index) AddProperties(addProperties map[string]*protocol.Property) {
	if len(addProperties) > 0 {
		index.lock.Lock()
		defer index.lock.Unlock()
		properties := make(map[string]*protocol.Property)
		for name, property := range index.Mappings.Properties {
			properties[name] = property
		}
		for name, addProperty := range addProperties {
			properties[name] = &protocol.Property{
				Type:    addProperty.Type,
				Dynamic: addProperty.Dynamic,
			}
		}
		index.Mappings.Properties = properties
	}
}

// GetShardByRouting
// TODO: build the real route, temporarily think that there is always only 1 shard
func (index *Index) GetShardByRouting() *Shard {
	for _, shard := range index.Shards {
		return shard
	}
	return nil
}

func (index *Index) GetReadersByTime(start, end int64) (indexlib.Reader, error) {
	segments := index.GetSegmentsByTime(start, end)
	if len(segments) == 0 {
		return nil, errs.ErrNoSegmentMatched
	}
	merged, err := MergeSegmentReader(indexlib.BuildConf(config.Cfg.Directory), segments...)
	if err != nil {
		return nil, errors.Wrap(err, "fail to merge multiple segment readers")
	}
	return merged, nil
}

func (index *Index) GetSegmentsByTime(start, end int64) []*Segment {
	var segments []*Segment
	for _, shard := range index.Shards {
		for _, segment := range shard.Segments {
			if segment.MatchTime(start, end) {
				segments = append(segments, segment)
			}
		}
	}
	logger.Info(
		"find segments",
		zap.String("index", index.Name),
		zap.Int64("start", start),
		zap.Int64("end", end),
		zap.Int("size", len(segments)),
	)
	return segments
}

func (index *Index) Close() error {
	for _, shard := range index.Shards {
		shard.Close()
	}

	// clear fs data files
	dp := path.Join(config.Cfg.GetFSPath(), consts.PathData, index.Name)
	err1 := os.RemoveAll(dp)

	// clear fs cache files
	cp := path.Join(config.Cfg.GetFSPath(), consts.PathCache, index.Name)
	err2 := os.RemoveAll(cp)

	if err1 != nil {
		logger.Error(
			"clear fs data files fail",
			zap.String("index", index.GetName()),
			zap.Error(err1),
		)
		return err1
	}

	if err2 != nil {
		logger.Error(
			"clear fs cache files fail",
			zap.String("index", index.GetName()),
			zap.Error(err2),
		)
		return err2
	}

	// clear oss data objects
	if strings.EqualFold(consts.DirectoryOSS, config.Cfg.Directory.Type) {
		defaultCli, err3 := oss.DefaultClient()
		if err3 != nil {
			return err3
		}
		objs, err4 := oss.ListObjects(
			defaultCli,
			config.Cfg.Directory.OSS.Bucket,
			oss.OssPath(index.GetName()),
		)
		if err4 != nil {
			return err4
		}
		if len(objs) > 0 {
			for _, obj := range objs {
				err5 := oss.DeleteObject(defaultCli, config.Cfg.Directory.OSS.Bucket, obj.Key)
				if err5 != nil {
					return err5
				}
			}
		}
	}

	return nil
}
