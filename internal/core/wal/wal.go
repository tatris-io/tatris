// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package wal organizes the entire Write-Ahead-Log program of Tatris
package wal

import (
	"encoding/json"
	"errors"
	"math"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/tatris-io/tatris/internal/core/config"

	"github.com/patrickmn/go-cache"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/common/utils"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/core/wal/log"
	"github.com/tatris-io/tatris/internal/core/wal/tidwall"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tidwall/wal"
	"go.uber.org/zap"
)

var wals *cache.Cache

func init() {
	wals = cache.New(cache.NoExpiration, cache.NoExpiration)
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			for range ticker.C {
				ConsumeWALs()
			}
		}
	}()
}

func OpenWAL(shard *core.Shard) (log.WalLog, error) {
	options := config.Cfg.Wal
	name := shard.GetName()
	defer utils.Timerf("consume wal finish, name:%s", name)()
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
	return twalLog, nil
}

func ProduceWAL(shard *core.Shard, docs []map[string]interface{}) error {
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
		wals.Set(name, wal, cache.NoExpiration)
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
	for name, wal := range wals.Items() {
		split := strings.Index(name, "/")
		i := name[:split]
		s, err := strconv.Atoi(name[split+1:])
		if err != nil {
			logger.Error(
				"parse wal name failed",
				zap.String("name", name),
				zap.Error(err),
			)
			continue
		}
		shard, err := metadata.GetShard(i, s)
		if shard == nil || err != nil {
			logger.Error("get shard failed", zap.String("name", name), zap.Error(err))
			continue
		}
		err = ConsumeWAL(shard, wal.Object.(log.WalLog))
		if err != nil {
			logger.Error(
				"consume shard wal failed",
				zap.String("name", name),
				zap.Error(err),
			)
			continue
		}
	}
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
	to = lastIndex
	if from == to {
		return nil
	}
	logger.Info(
		"consume shard wal start",
		zap.String("name", name),
		zap.Uint64("from", from),
		zap.Uint64("to", to),
	)
	docs := make([]map[string]interface{}, 0)
	for i := from; i <= to; i++ {
		l, err := wal.Read(i)
		if err != nil {
			return err
		}
		var doc map[string]interface{}
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
		zap.Uint64("size", to-from),
	)
	return nil
}

func persistDocs(shard *core.Shard,
	docs map[string]map[string]interface{}, minTime, maxTime time.Time) error {
	shard.CheckSegments()
	segment := shard.GetLatestSegment()
	if segment == nil {
		return errors.New("segment not found")
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
	err = writer.Batch(docs, shard.Index.Mappings)
	if err != nil {
		return err
	}
	segment.UpdateStat(minTime, maxTime, int64(len(docs)))
	return nil
}

func buildDocs(
	index *core.Index,
	docs []map[string]interface{},
) (map[string]map[string]interface{}, time.Time, time.Time, error) {
	idDocs := make(map[string]map[string]interface{})
	minTime, maxTime := time.UnixMilli(0), time.UnixMilli(math.MaxInt64)
	for _, doc := range docs {
		docID := ""
		docTimestamp := time.Now()
		if id, ok := doc[consts.IDField]; ok && id != nil && id != "" {
			docID = id.(string)
		} else {
			docID = utils.GenerateID()
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
		err := index.CheckMapping(docID, doc)
		if err != nil {
			return idDocs, minTime, maxTime, err
		}
		idDocs[docID] = doc
	}
	return idDocs, minTime, maxTime, nil
}
