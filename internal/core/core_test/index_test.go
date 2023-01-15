// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package core_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
	"github.com/tatris-io/tatris/internal/query"
	"github.com/tatris-io/tatris/test/prepare"
	"testing"
	"time"
)

const (
	indexPath = "../../../test/materials/index.json"
	docsPath  = "../../../test/materials/docs.json"
)

func TestIndex(t *testing.T) {

	// prepare
	start := time.Now()
	version := start.Format(consts.VersionTimeFmt)
	index, err := prepare.PrepareIndex(indexPath, version)
	if err != nil {
		t.Fatalf("prepare index fail: %s", err.Error())
	}
	docs, err := prepare.PrepareDocs(index.Name, docsPath)
	if err != nil {
		t.Fatalf("prepare docs fail: %s", err.Error())
	}

	// test
	t.Run("test_index", func(t *testing.T) {
		index, err := metadata.GetIndex(index.Name)
		assert.NoError(t, err)
		assert.NotNil(t, index)
		assert.Equal(t, index.Settings.NumberOfShards, index.GetShardNum())
		assert.Equal(t, index.Settings.NumberOfShards, len(index.GetShards()))
		for i := 0; i < index.GetShardNum(); i++ {
			assert.NotNil(t, index.GetShard(i))
		}
		assert.NotNil(t, index.GetShardByRouting())
		readers, err := index.GetReadersByTime(start.Unix(), time.Now().UnixMilli())
		assert.NoError(t, err)
		assert.Equal(t, len(docs)/10, len(readers))

		hits, err := query.SearchDocs(protocol.QueryRequest{Index: index.Name, Query: protocol.Query{Term: protocol.Term{"name": "elasticsearch"}}, Size: 20})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(hits.Hits))
	})
}
