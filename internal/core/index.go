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

	"github.com/sourcegraph/conc/pool"

	"github.com/tatris-io/tatris/internal/common/utils"

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

func (index *Index) Destroy() error {

	defer utils.Timerf("close index finish, name:%s", index.GetName())()

	// destroy shards

	for _, shard := range index.Shards {
		if err := shard.Destroy(); err != nil {
			return err
		}
	}

	// clear fs data dir
	dp := path.Join(config.Cfg.GetFSPath(), consts.PathData, index.GetName())
	err1 := os.RemoveAll(dp)

	// clear fs cache dir
	cp := path.Join(config.Cfg.GetFSPath(), consts.PathCache, index.GetName())
	err2 := os.RemoveAll(cp)

	// clear fs wal dir
	wp := path.Join(config.Cfg.GetFSPath(), consts.PathWAL, index.GetName())
	err3 := os.RemoveAll(wp)

	if err1 != nil {
		logger.Error(
			"clear fs data dir fail",
			zap.String("index", index.GetName()),
			zap.Error(err1),
		)
		return err1
	}

	if err2 != nil {
		logger.Error(
			"clear fs cache dir fail",
			zap.String("index", index.GetName()),
			zap.Error(err2),
		)
		return err2
	}

	if err3 != nil {
		logger.Error(
			"clear fs wal dir fail",
			zap.String("index", index.GetName()),
			zap.Error(err3),
		)
		return err3
	}

	// clear oss data objects
	if strings.EqualFold(consts.DirectoryOSS, config.Cfg.Directory.Type) {
		var err error
		defaultCli, err := oss.DefaultClient()
		if err == nil {
			objs, err := oss.ListObjects(
				defaultCli,
				config.Cfg.Directory.OSS.Bucket,
				oss.OssPath(index.GetName()),
			)
			if err == nil {
				if len(objs) > 0 {
					n := (len(objs) + oss.MaxKeySize - 1) / oss.MaxKeySize
					objGroups := make([][]string, n)
					for i, obj := range objs {
						group := i / oss.MaxKeySize
						objGroups[group] = append(objGroups[group], obj.Key)
					}

					p := pool.New().WithErrors().WithMaxGoroutines(n)

					for _, objGroup := range objGroups {
						og := objGroup
						p.Go(func() error {
							return oss.DeleteObjects(
								defaultCli,
								config.Cfg.Directory.OSS.Bucket,
								og,
							)
						})
					}

					err = p.Wait()
				}
			}
		}
		if err != nil {
			logger.Error(
				"clear oss objects fail",
				zap.String("index", index.GetName()),
				zap.Error(err),
			)
			return err
		}
	}

	return nil
}
