// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package wal_test

import (
	"strings"
	"testing"
	"time"

	"github.com/tatris-io/tatris/internal/common/consts"

	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/ingestion"
	"github.com/tatris-io/tatris/internal/protocol"
	"github.com/tatris-io/tatris/internal/query"
	"github.com/tatris-io/tatris/test/ut/prepare"
)

func TestWal(t *testing.T) {
	index, err := prepare.CreateIndex(
		strings.ReplaceAll(
			time.Now().Format(consts.TimeFmtWithoutSeparator),
			consts.Dot,
			consts.Empty,
		),
	)
	assert.NoError(t, err)
	assert.NotNil(t, index)

	for i := 0; i < 5; i++ {
		// insert one doc
		err = ingestion.IngestDocs(index, []protocol.Document{
			{
				"test": "1",
			},
		})
		assert.NoError(t, err)
		// sleep 2s to wait wal done
		time.Sleep(2 * time.Second)
		// docs are visible now
		resp, err := query.SearchDocs([]*core.Index{index}, protocol.QueryRequest{
			Index: index.Name,
			Query: protocol.Query{
				MatchAll: &protocol.MatchAll{},
			},
			Size: 9999,
		})
		assert.NoError(t, err)
		assert.Equal(t, int64(i+1), resp.Hits.Total.Value)
	}
}
