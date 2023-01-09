// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package bluge organizes codes of the indexing library bluge
package bluge

import (
	"context"
	"encoding/json"
	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/bluge/config"
	"log"
	"time"
)

type BlugeReader struct {
	*indexlib.BaseConfig
	Reader *bluge.Reader
}

func NewBlugeReader(config *indexlib.BaseConfig) *BlugeReader {
	return &BlugeReader{BaseConfig: config}
}

func (b *BlugeReader) OpenReader() error {
	var cfg bluge.Config

	switch b.StorageType {
	case indexlib.FSStorageType:
		cfg = config.GetFSConfig(b.DataPath, b.Index)
	default:
		cfg = config.GetFSConfig(b.DataPath, b.Index)
	}

	reader, err := bluge.OpenReader(cfg)
	if err != nil {
		return err
	}

	b.Reader = reader
	return nil
}

func (b *BlugeReader) Search(ctx context.Context, query indexlib.QueryRequest, limit int) (*indexlib.QueryResponse, error) {
	blugeQuery := b.generateQuery(query)
	var searchRequest bluge.SearchRequest

	if limit == -1 {
		searchRequest = bluge.NewAllMatches(blugeQuery).WithStandardAggregations()
	} else {
		searchRequest = bluge.NewTopNSearch(limit, blugeQuery).WithStandardAggregations()
	}

	dmi, err := b.Reader.Search(ctx, searchRequest)
	if err != nil {
		log.Printf("bluge all match error: %s", err)
		return nil, err
	}

	return b.generateResponse(dmi), nil
}

func (b *BlugeReader) generateQuery(query indexlib.QueryRequest) bluge.Query {
	var blugeQuery bluge.Query

	switch query := query.(type) {
	case *indexlib.MatchAllQuery:
		q := bluge.NewMatchAllQuery()
		blugeQuery = q
	case *indexlib.MatchQuery:
		q := bluge.NewMatchQuery(query.Match)
		if query.Field != "" {
			q.SetField(query.Field)
		}
		blugeQuery = q
	case *indexlib.TermQuery:
		q := bluge.NewTermQuery(query.Term)
		if query.Field != "" {
			q.SetField(query.Field)
		}
		blugeQuery = q
	case *indexlib.BooleanQuery:
		q := bluge.NewBooleanQuery()
		if query.Musts != nil {
			for _, must := range query.Musts {
				q.AddMust(b.generateQuery(must))
			}
		}
		if query.MustNots != nil {
			for _, mustNot := range query.MustNots {
				q.AddMustNot(b.generateQuery(mustNot))
			}
		}
		if query.Shoulds != nil {
			for _, should := range query.Shoulds {
				q.AddShould(b.generateQuery(should))
			}
		}
	case *indexlib.TermsQuery:
		q := bluge.NewBooleanQuery()
		for k, v := range query.Terms {
			field := k
			subBooleanQuery := bluge.NewBooleanQuery()
			for _, vv := range v.Fields {
				subq := bluge.NewTermQuery(vv).SetField(field)
				subBooleanQuery.AddShould(subq)
			}
			q.AddMust(subBooleanQuery)
		}
		blugeQuery = q
	case *indexlib.RangeQuery:
		q, err := RangeQuery(query)
		if err != nil {
			log.Printf("bluge range query error: %s", err)
			return nil
		}
		blugeQuery = q
	}

	return blugeQuery
}

func (b *BlugeReader) generateResponse(dmi search.DocumentMatchIterator) *indexlib.QueryResponse {
	Hits := make([]indexlib.Hit, 0)
	next, err := dmi.Next()
	for err == nil && next != nil {
		var id string
		var index string
		var source map[string]interface{}
		var timestamp time.Time

		err = next.VisitStoredFields(func(field string, value []byte) bool {
			switch field {
			case consts.TimestampField:
				location, _ := time.LoadLocation("Asia/Shanghai")
				timestamp, _ = bluge.DecodeDateTime(value)
				timestamp = timestamp.In(location)
			case consts.IDField:
				id = string(value)
			case consts.IndexField:
				index = string(value)
			case consts.SourceField:
				err := json.Unmarshal(value, &source)
				if err != nil {
					log.Printf("bluge source unmarshal error: %s", err)
				}
			}
			return true
		})
		if err != nil {
			log.Printf("bluge VisitStored error: %s", err)
			continue
		}

		hit := indexlib.Hit{
			Index:     index,
			ID:        id,
			Source:    source,
			Timestamp: timestamp,
		}
		Hits = append(Hits, hit)

		next, err = dmi.Next()
	}

	resp := &indexlib.QueryResponse{
		Took: dmi.Aggregations().Duration().Milliseconds(),
		Hits: indexlib.Hits{
			Total: indexlib.Total{Value: int64(dmi.Aggregations().Count())},
			Hits:  Hits,
		},
	}

	return resp
}

func (b *BlugeReader) Close() {
	if b.Reader != nil {
		err := b.Reader.Close()
		if err != nil {
			log.Printf("fail to close bluge reader for: %s", err)
		}
	}
}
