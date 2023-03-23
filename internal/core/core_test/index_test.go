// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package core_test

import (
	"github.com/tatris-io/tatris/internal/common/consts"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/core/config"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
	"github.com/tatris-io/tatris/internal/query"
	"github.com/tatris-io/tatris/test/ut/prepare"
)

func TestIndex(t *testing.T) {

	// prepare
	start := time.Now()
	index, docs, err := prepare.CreateIndexAndDocs(
		strings.ReplaceAll(
			start.Format(consts.TimeFmtWithoutSeparator),
			consts.Dot,
			consts.Empty,
		),
	)
	if err != nil {
		t.Fatalf("prepare docs fail: %s", err.Error())
	}

	// test
	t.Run("test_index", func(t *testing.T) {
		index, err := metadata.GetIndexExplicitly(index.Name)
		assert.NoError(t, err)
		assert.NotNil(t, index)
		assert.Equal(t, index.Settings.NumberOfShards, index.GetShardNum())
		assert.Equal(t, index.Settings.NumberOfShards, len(index.GetShards()))
		for i := 0; i < index.GetShardNum(); i++ {
			assert.NotNil(t, index.GetShard(i))
		}
		assert.NotNil(t, index.GetShardByRouting())
		reader, err := index.GetReadersByTime(start.Unix(), time.Now().UnixMilli())
		if reader != nil {
			defer reader.Close()
		}
		assert.NoError(t, err)
		assert.Equal(
			t,
			(int)(math.Ceil((float64(len(docs)))/(float64(config.Cfg.Segment.MatureThreshold)))),
			reader.Count(),
		)

		resp, err := query.SearchDocs(
			[]*core.Index{index}, protocol.QueryRequest{
				Index: index.Name,
				Query: protocol.Query{Term: protocol.Term{"name": "elasticsearch"}},
				Size:  20,
			},
		)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(resp.Hits.Hits))
	})
}

func TestSegmentReader(t *testing.T) {
	// prepare
	start := time.Now()
	index, _, err := prepare.CreateIndexAndDocs(
		strings.ReplaceAll(
			start.Format(consts.TimeFmtWithoutSeparator),
			consts.Dot,
			consts.Empty,
		),
	)
	if err != nil {
		t.Fatalf("prepare docs fail: %s", err.Error())
	}
	segment := index.Shards[0].GetLatestSegment()

	writer, err := segment.GetWriter()
	assert.NoError(t, err)
	assert.NotNil(t, writer)

	reader1, err := segment.GetReader()
	assert.NoError(t, err)
	assert.NotNil(t, reader1)

	reader2, err := segment.GetReader()
	assert.NoError(t, err)
	assert.NotNil(t, reader2)

	assert.Equal(t, core.SegmentStatusWritable, segment.Status())

	index.Shards[0].ForceAddSegment()
	assert.True(t, segment.Readonly())

	_, err = segment.GetWriter()
	assert.Same(t, err, errs.ErrSegmentReadonly)

	assert.Equal(t, core.SegmentStatusReadonly, segment.Status())

	reader1.Close()
	reader2.Close()

	assert.Equal(t, core.SegmentStatusReadonly, segment.Status())
}
