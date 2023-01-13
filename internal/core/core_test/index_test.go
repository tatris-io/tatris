// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package core_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/core"
	"github.com/tatris-io/tatris/internal/ingestion"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
	"github.com/tatris-io/tatris/internal/query"
	"testing"
	"time"
)

func TestIndex(t *testing.T) {

	t.Run("prepare_meta", func(t *testing.T) {
		index := &core.Index{
			Index: &protocol.Index{
				Name:     "storage_product",
				Settings: &protocol.Settings{NumberOfShards: 2, NumberOfReplicas: 1},
				Mappings: &protocol.Mappings{
					Properties: map[string]protocol.Property{
						"name": {Type: "keyword"},
						"desc": {Type: "text"}},
				}},
			Shards: []*core.Shard{}}
		err := metadata.CreateIndex(index)
		assert.Nil(t, err)
	})

	t.Run("prepare_docs", func(t *testing.T) {
		docs := []map[string]interface{}{
			{"name": "tatris", "desc": "Time-aware storage and search system"}, {"name": "mysql", "desc": "Relational database"}, {"name": "elasticsearch", "desc": "Distributed, RESTful search and analytics engine"}, {"name": "mongodb", "desc": "Source-available cross-platform document-oriented database program"}, {"name": "redis", "desc": "Open source (BSD licensed), in-memory data structure store"}, {"name": "hbase", "desc": "Distributed, scalable, big data store"},
		}
		err := ingestion.IngestDocs("storage_product", docs)
		assert.Nil(t, err)
		time.Sleep(3 * time.Second)
		docs = []map[string]interface{}{
			{"name": "tatris", "desc": "Time-aware storage and search system"}, {"name": "mysql", "desc": "Relational database"}, {"name": "elasticsearch", "desc": "Distributed, RESTful search and analytics engine"}, {"name": "mongodb", "desc": "Source-available cross-platform document-oriented database program"}, {"name": "redis", "desc": "Open source (BSD licensed), in-memory data structure store"}, {"name": "hbase", "desc": "Distributed, scalable, big data store"},
		}
		err = ingestion.IngestDocs("storage_product", docs)
		assert.Nil(t, err)
	})

	t.Run("test_ingest", func(t *testing.T) {
		index, err := metadata.GetIndex("storage_product")
		assert.Nil(t, err)
		assert.NotNil(t, index)
		assert.Equal(t, 2, index.GetShardNum())
		assert.Equal(t, 2, len(index.GetShards()))
		for i := 0; i < index.GetShardNum(); i++ {
			assert.NotNil(t, index.GetShard(i))
		}
		assert.NotNil(t, index.GetShardByRouting())
		readers, err := index.GetReadersByTime(time.Now().UnixMilli()-6000, time.Now().UnixMilli())
		assert.Nil(t, err)
		assert.Equal(t, 2, len(readers))
		readers, err = index.GetReadersByTime(time.Now().UnixMilli()-3000, time.Now().UnixMilli())
		assert.Nil(t, err)
		assert.Equal(t, 1, len(readers))
	})

	t.Run("test_query", func(t *testing.T) {

		index, err := metadata.GetIndex("storage_product")
		assert.Nil(t, err)
		assert.NotNil(t, index)

		hits, err := query.SearchDocs(protocol.QueryRequest{Index: "storage_product", Query: protocol.Query{Term: protocol.Term{"name": "tatris"}}, Size: 20})
		assert.Nil(t, err)
		assert.GreaterOrEqual(t, len(hits.Hits), 2)
	})
}
