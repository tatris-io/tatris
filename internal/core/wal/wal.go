// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package wal organizes the entire Write-Ahead-Log program of Tatris
package wal

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tatris-io/tatris/internal/protocol"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/sourcegraph/conc/pool"
	"github.com/tatris-io/tatris/internal/core/config"

	"github.com/patrickmn/go-cache"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/common/utils"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/core/wal/log"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"go.uber.org/zap"
)

const (
	consumptionLimit = 5000
)

var wals *cache.Cache

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
	wal, err := shard.OpenWAL()
	if err != nil {
		return nil, err
	}
	wals.Set(name, wal, cache.NoExpiration)
	return wal, nil
}

func ProduceWAL(shard *core.Shard, docs []protocol.Document) error {
	name := shard.GetName()
	defer utils.Timerf("produce wal finish, name:%s, size:%d", name, len(docs))()
	w, found := wals.Get(name)
	var wal log.WalLog
	var err error
	if found {
		wal = w.(log.WalLog)
	} else {
		if wal, err = OpenWAL(shard); err != nil {
			return err
		}
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
	items := wals.Items()
	defer utils.Timerf("consume wals finish, size:%d", len(items))()
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
			shard, err := metadata.GetShard(i, s)
			if shard == nil || err != nil {
				logger.Error("get shard failed", zap.String("name", n), zap.Error(err))
				return
			}
			err = ConsumeWAL(shard, w.Object.(log.WalLog))
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
	firstIndex, err := wal.FirstIndex()
	if err != nil {
		return err
	}
	lastIndex, err := wal.LastIndex()
	if err != nil {
		return err
	}
	var from, to uint64
	if shard.Stat.WalIndex == 0 {
		from = 1
	} else {
		from = uint64(math.Max(float64(firstIndex), float64(shard.Stat.WalIndex))) + 1
	}
	to = uint64(math.Min(float64(lastIndex), float64(from+consumptionLimit-1)))
	if from >= to {
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

	idDocs, minTime, maxTime, err := buildDocs(shard.Index, docs)
	if err != nil {
		return err
	}
	err = persistDocs(shard, idDocs, minTime, maxTime)
	if err != nil {
		return err
	}
	shard.UpdateStat(minTime, maxTime, int64(len(docs)), to)
	err = metadata.SaveIndex(shard.Index)
	if err != nil {
		return err
	}
	// The id passed to func TruncateFront cannot be greater than the last index of the stock log
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

func persistDocs(shard *core.Shard,
	docs map[string]protocol.Document, minTime, maxTime time.Time) error {
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
	logger.Info(
		"ready to persist docs",
		zap.String("index", shard.Index.Name),
		zap.Int("shard", shard.ShardID),
		zap.Int("segment", segment.SegmentID),
		zap.Int("size", len(docs)),
	)
	err = writer.Batch(docs)
	if err != nil {
		return err
	}
	segment.UpdateStat(minTime, maxTime, int64(len(docs)))
	return nil
}

func buildDocs(
	index *core.Index,
	docs []protocol.Document,
) (map[string]protocol.Document, time.Time, time.Time, error) {
	idDocs := make(map[string]protocol.Document)
	minTime, maxTime := time.UnixMilli(math.MaxInt64), time.UnixMilli(0)
	for _, doc := range docs {
		docID := ""
		docTimestamp := time.Now()
		if id, ok := doc[consts.IDField]; ok && id != nil && id != "" {
			docID = id.(string)
		} else {
			genID, err := utils.GenerateID()
			if err != nil {
				return idDocs, minTime, maxTime, err
			}
			docID = genID
		}
		if timestamp, ok := doc[consts.TimestampField]; ok && timestamp != nil {
			docTimestamp = timestamp.(time.Time)
		}
		if docTimestamp.Before(minTime) {
			minTime = docTimestamp
		}
		if docTimestamp.After(maxTime) {
			maxTime = docTimestamp
		}
		doc[consts.IDField] = docID
		doc[consts.TimestampField] = docTimestamp
		err := index.CheckMapping(doc)
		if err != nil {
			return idDocs, minTime, maxTime, err
		}
		idDocs[docID] = doc
	}
	return idDocs, minTime, maxTime, nil
}
