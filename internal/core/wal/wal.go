// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package wal organizes the entire Write-Ahead-Log program of Tatris
package wal

import (
	"encoding/json"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tatris-io/tatris/internal/core"

	"github.com/tatris-io/tatris/internal/core/wal/tidwall"
	"github.com/tidwall/wal"

	"github.com/tatris-io/tatris/internal/protocol"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/sourcegraph/conc/pool"
	"github.com/tatris-io/tatris/internal/core/config"

	"github.com/patrickmn/go-cache"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/common/utils"
	"github.com/tatris-io/tatris/internal/core/wal/log"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"go.uber.org/zap"
)

const (
	consumptionLimit = 5000
)

var (
	wals *cache.Cache
	lock sync.Mutex
)

func init() {
	wals = cache.New(cache.NoExpiration, cache.NoExpiration)
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			ConsumeWALs()
		}
	}()
}

func OpenWAL(shard *core.Shard) (log.WalLog, error) {
	name := shard.GetName()
	defer utils.Timerf("open wal finish, name:%s", name)()

	options := config.Cfg.Wal
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
	shard.Wal = twalLog

	if err != nil {
		return nil, err
	}
	wals.Set(name, twalLog, cache.NoExpiration)
	return twalLog, nil
}

func ProduceWAL(shard *core.Shard, docs []protocol.Document) error {
	name := shard.GetName()
	defer utils.Timerf("produce wal finish, name:%s, size:%d", name, len(docs))()
	wal := shard.Wal
	var err error
	if wal == nil {
		lock.Lock()
		wal = shard.Wal
		if wal == nil {
			if wal, err = OpenWAL(shard); err != nil {
				return err
			}
		}
		lock.Unlock()
	}
	datas := make([][]byte, 0)
	for _, doc := range docs {
		data, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		datas = append(datas, data)
	}
	return wal.BWrite(datas)
}

func ConsumeWALs() {
	p := pool.New().WithMaxGoroutines(config.Cfg.Wal.Parallel)
	defer utils.Timerf("consume wals finish")()
	lock.Lock()
	defer lock.Unlock()
	items := wals.Items()
	for name, wal := range items {
		n := name
		w := wal
		p.Go(func() {
			split := strings.Index(n, "/")
			i := n[:split]
			s, err := strconv.Atoi(n[split+1:])
			if err != nil {
				logger.Error(
					"parse wal name failed",
					zap.String("name", n),
					zap.Error(err),
				)
				return
			}
			wallog := w.Object.(log.WalLog)
			shard, err := metadata.GetShard(i, s)
			if err != nil {
				if errs.IsIndexNotFound(err) || errs.IsShardNotFound(err) {
					// index or shard has been deleted, clear wal
					wals.Delete(n)
					wallog.Close()
					var p string
					if errs.IsIndexNotFound(err) {
						p = path.Join(consts.DefaultWALPath, i)
					} else {
						p = path.Join(consts.DefaultWALPath, n)
					}
					err = os.RemoveAll(p)
					if err != nil {
						logger.Error("clean wal failed", zap.String("name", n), zap.Error(err))
					}
				} else {
					logger.Error("get shard failed", zap.String("name", n), zap.Error(err))
				}
				return
			}
			err = ConsumeWAL(shard, wallog)
			if err != nil {
				logger.Error(
					"consume shard wal failed",
					zap.String("name", n),
					zap.Error(err),
				)
				return
			}
		})
	}
	p.Wait()
}

func ConsumeWAL(shard *core.Shard, wal log.WalLog) error {
	name := shard.GetName()
	defer utils.Timerf("consume wal finish, name:%s", name)()

	lastIndex, err := wal.LastIndex()
	if err != nil {
		return err
	}

	// no new wal to read
	if shard.Stat.WalIndex >= lastIndex {
		return nil
	}

	// Because we always call 'wal.TruncateFront(to)' before this func returns.
	// So firstIndex is always the last consumed index, or firstIndex is 1 when first time wal read.
	firstIndex, err := wal.FirstIndex()
	if err != nil {
		return err
	}

	// from is the first index we need to consume
	from := shard.Stat.WalIndex + 1

	// If all is OK, we have firstIndex == shard.Stat.WalIndex

	if firstIndex != shard.Stat.WalIndex {
		// (firstIndex == 1 && shard.Stat.WalIndex == 0) is expected when first time wal read
		if !(firstIndex == 1 && shard.Stat.WalIndex == 0) {
			if from < firstIndex {
				// from jumps to firstIndex
				from = firstIndex
				logger.Warn(
					"[wal] maybe loss wal",
					zap.String("shard", shard.GetName()),
					zap.Uint64("from", from),
					zap.Uint64("to", firstIndex-1),
				)
			} else {
				logger.Warn("[wal] last truncate may fail", zap.String("shard", shard.GetName()), zap.Uint64("from", firstIndex), zap.Uint64("to", shard.Stat.WalIndex))
			}
		}
	}

	// from is the last index we need to consume
	to := lastIndex
	if to > from+consumptionLimit-1 {
		to = from + consumptionLimit - 1
	}

	if from > to {
		return nil
	}
	logger.Info(
		"consume shard wal start",
		zap.String("name", name),
		zap.Uint64("from", from),
		zap.Uint64("to", to),
	)
	docs := make([]protocol.Document, 0)
	for i := from; i <= to; i++ {
		l, err := wal.Read(i)
		if err != nil {
			return err
		}
		var doc protocol.Document
		err = json.Unmarshal(l, &doc)
		if err != nil {
			return err
		}
		docs = append(docs, doc)
	}

	err = persistDocuments(shard, docs, to)
	if err != nil {
		return err
	}
	// The id passed to func TruncateFront cannot be greater than the last index of the stock log.
	// There is no way to clear wal. Once data is written to wal, there is always at least one entry
	// in the wal.
	err = wal.TruncateFront(to)
	if err != nil {
		return err
	}
	logger.Info(
		"consume shard wal success",
		zap.String("name", name),
		zap.Uint64("from", from),
		zap.Uint64("to", to),
		zap.Uint64("size", to-from+1),
	)
	return nil
}

func persistDocuments(shard *core.Shard,
	docs []protocol.Document, walIndex uint64) error {
	shard.CheckSegments()
	segment := shard.GetLatestSegment()
	if segment == nil {
		return &errs.NoSegmentError{
			Index: shard.Index.Name,
			Shard: shard.ShardID,
		}
	}
	writer, err := segment.GetWriter()
	if err != nil {
		return err
	}
	minTime, maxTime := time.UnixMilli(math.MaxInt64), time.UnixMilli(0)
	idDocs := make(map[string]protocol.Document)
	for _, doc := range docs {
		docID := doc[consts.IDField].(string)
		docTimestamp, err := utils.ParseTime(doc[consts.TimestampField])
		if err != nil {
			return err
		}
		if docTimestamp.Before(minTime) {
			minTime = docTimestamp
		}
		if docTimestamp.After(maxTime) {
			maxTime = docTimestamp
		}
		idDocs[docID] = doc
	}
	logger.Info(
		"ready to persist docs",
		zap.String("index", shard.Index.Name),
		zap.Int("shard", shard.ShardID),
		zap.Int("segment", segment.SegmentID),
		zap.Int("size", len(idDocs)),
		zap.Time("minTime", minTime),
		zap.Time("maxTime", maxTime),
	)
	err = writer.Batch(idDocs)
	if err != nil {
		return err
	}
	segment.UpdateStat(minTime, maxTime, int64(len(docs)))
	shard.UpdateStat(minTime, maxTime, int64(len(docs)), walIndex)
	err = metadata.SaveIndex(shard.Index)
	if err != nil {
		return err
	}
	return nil
}
