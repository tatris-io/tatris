// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package bluge organizes codes of the indexing library bluge
package bluge

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/blugelabs/bluge/search/aggregations"

	"github.com/tatris-io/tatris/internal/common/utils"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/analysis"
	"github.com/blugelabs/bluge/analysis/analyzer"
	"github.com/blugelabs/bluge/search"
	qs "github.com/blugelabs/query_string"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/bluge/config"
)

type BlugeReader struct {
	*indexlib.BaseConfig
	Indexes []string
	Readers []*bluge.Reader
}

func NewBlugeReader(config *indexlib.BaseConfig, index ...string) *BlugeReader {
	return &BlugeReader{BaseConfig: config, Indexes: index, Readers: make([]*bluge.Reader, 0)}
}

func (b *BlugeReader) OpenReader() error {
	var cfg bluge.Config

	for _, index := range b.Indexes {
		switch b.StorageType {
		case indexlib.FSStorageType:
			cfg = config.GetFSConfig(b.DataPath, index)
		default:
			cfg = config.GetFSConfig(b.DataPath, index)
		}

		reader, err := bluge.OpenReader(cfg)
		if err != nil {
			return err
		}
		b.Readers = append(b.Readers, reader)
	}

	return nil
}

func (b *BlugeReader) Search(
	ctx context.Context,
	query indexlib.QueryRequest,
	limit int,
) (*indexlib.QueryResponse, error) {
	defer utils.Timerf(
		"bluge search docs finish, index:%+v, query:%+v, limit:%d",
		b.Indexes,
		query,
		limit,
	)()
	blugeQuery, err := b.generateQuery(query)
	if err != nil {
		return nil, err
	}

	if limit < 0 {
		limit = 10
	}
	searchRequest := bluge.NewTopNSearch(limit, blugeQuery).WithStandardAggregations()
	if querySorts := query.GetSort(); querySorts != nil {
		sorts := make([]*search.Sort, 0, len(querySorts))
		for _, querySort := range querySorts {
			for k, v := range querySort {
				sort := search.SortBy(search.Field(k))
				if strings.EqualFold("desc", v.Order) {
					sort.Desc()
				}
				if strings.EqualFold("_first", v.Missing) {
					sort.MissingFirst()
				}
				sorts = append(sorts, sort)
			}
		}
		searchRequest.SortByCustom(sorts)
	}
	if aggs := query.GetAggs(); aggs != nil {
		blugeAggs := b.generateAggregations(aggs)
		for name, agg := range blugeAggs {
			searchRequest.AddAggregation(name, agg)
		}
	}

	docs := make([]*search.DocumentMatch, 0)
	var bucket *search.Bucket

	for _, reader := range b.Readers {
		dmi, err := reader.Search(ctx, searchRequest)
		if err != nil {
			log.Printf("bluge all match error: %s", err)
			return nil, err
		}
		next, err := dmi.Next()
		for err == nil && next != nil {
			docs = append(docs, next)
			next, err = dmi.Next()
		}

		// merge aggregation response
		if bucket == nil {
			bucket = dmi.Aggregations()
		} else {
			bucket.Merge(dmi.Aggregations())
		}
	}
	bucket.Aggregation("duration").Finish()

	return b.generateResponse(docs, bucket)
}

func (b *BlugeReader) generateQuery(query indexlib.QueryRequest) (bluge.Query, error) {
	var blugeQuery bluge.Query

	switch query := query.(type) {
	case *indexlib.MatchAllQuery:
		q := bluge.NewMatchAllQuery()
		blugeQuery = q
	case *indexlib.MatchQuery:
		q, err := b.generateMatchQuery(query)
		if err != nil {
			return nil, err
		}
		blugeQuery = q
	case *indexlib.MatchPhraseQuery:
		q, err := b.generateMatchPhraseQuery(query)
		if err != nil {
			return nil, err
		}
		blugeQuery = q
	case *indexlib.QueryString:
		q, err := b.generateQueryString(query)
		if err != nil {
			return nil, err
		}
		blugeQuery = q
	case *indexlib.TermQuery:
		q := bluge.NewTermQuery(query.Term)
		if query.Field != "" {
			q.SetField(query.Field)
		}
		blugeQuery = q
	case *indexlib.BooleanQuery:
		q, err := b.generateBoolQuery(query)
		if err != nil {
			return nil, err
		}
		blugeQuery = q
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
		q, err := RangeQueryParse(query)
		if err != nil {
			return nil, err
		}
		blugeQuery = q
	}

	return blugeQuery, nil
}

func (b *BlugeReader) generateResponse(
	docs []*search.DocumentMatch,
	buckets *search.Bucket,
) (*indexlib.QueryResponse, error) {
	Hits := make([]indexlib.Hit, 0)
	for _, doc := range docs {
		var id string
		var index string
		var source map[string]interface{}
		var timestamp time.Time

		err := doc.VisitStoredFields(func(field string, value []byte) bool {
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
	}

	aggsResponse, err := b.generateAggsResponse(buckets)
	if err != nil {
		return nil, err
	}

	resp := &indexlib.QueryResponse{
		Took: buckets.Duration().Milliseconds(),
		Hits: indexlib.Hits{
			Total: indexlib.Total{Value: int64(buckets.Count())},
			Hits:  Hits,
		},
		Aggregations: aggsResponse,
	}

	return resp, nil
}

func (b *BlugeReader) generateMatchQuery(query *indexlib.MatchQuery) (bluge.Query, error) {
	q := bluge.NewMatchQuery(query.Match)
	if query.Field != "" {
		q.SetField(query.Field)
	}
	if query.Prefix != 0 {
		q.SetPrefix(query.Prefix)
	}
	if query.Fuzziness != 0 {
		q.SetFuzziness(query.Fuzziness)
	}
	if query.Operator != "" {
		switch strings.ToUpper(query.Operator) {
		case "OR":
			q.SetOperator(bluge.MatchQueryOperatorOr)
		case "AND":
			q.SetOperator(bluge.MatchQueryOperatorAnd)
		}
	}
	analyzer := b.generateAnalyzer(query.Analyzer)
	if analyzer != nil {
		q.SetAnalyzer(analyzer)
	}

	return q, nil
}

func (b *BlugeReader) generateMatchPhraseQuery(
	query *indexlib.MatchPhraseQuery,
) (bluge.Query, error) {
	q := bluge.NewMatchPhraseQuery(query.MatchPhrase)
	if query.Field != "" {
		q.SetField(query.Field)
	}
	if query.Slop != 0 {
		q.SetSlop(query.Slop)
	}

	return q, nil
}

func (b *BlugeReader) generateQueryString(query *indexlib.QueryString) (bluge.Query, error) {
	options := qs.DefaultOptions()
	analyzer := b.generateAnalyzer(query.Analyzer)
	if analyzer != nil {
		options.WithDefaultAnalyzer(analyzer)
	}

	return qs.ParseQueryString(query.Query, options)
}

func (b *BlugeReader) generateAnalyzer(analyzerStr string) *analysis.Analyzer {
	if analyzerStr != "" {
		switch strings.ToUpper(analyzerStr) {
		case "KEYWORD":
			return analyzer.NewKeywordAnalyzer()
		case "SIMPLE":
			return analyzer.NewSimpleAnalyzer()
		case "STANDARD":
			return analyzer.NewStandardAnalyzer()
		case "WEB":
			return analyzer.NewWebAnalyzer()
		default:
			return analyzer.NewStandardAnalyzer()
		}
	}
	return nil
}

func (b *BlugeReader) generateBoolQuery(query *indexlib.BooleanQuery) (bluge.Query, error) {
	q := bluge.NewBooleanQuery()
	if query.Musts != nil {
		for _, must := range query.Musts {
			tmpQuery, err := b.generateQuery(must)
			if err != nil {
				return nil, err
			}
			q.AddMust(tmpQuery)
		}
	}
	if query.MustNots != nil {
		for _, mustNot := range query.MustNots {
			tmpQuery, err := b.generateQuery(mustNot)
			if err != nil {
				return nil, err
			}
			q.AddMustNot(tmpQuery)
		}
	}
	if query.Shoulds != nil {
		for _, should := range query.Shoulds {
			tmpQuery, err := b.generateQuery(should)
			if err != nil {
				return nil, err
			}
			q.AddShould(tmpQuery)
		}
	}
	if query.Filters != nil {
		filter := bluge.NewBooleanQuery().SetBoost(0)
		for _, fliter := range query.Filters {
			tmpQuery, err := b.generateQuery(fliter)
			if err != nil {
				return nil, err
			}
			filter.AddMust(tmpQuery)
		}
		q.AddMust(filter)
	}
	q.SetMinShould(query.MinShould)
	return q, nil
}

func (b *BlugeReader) generateAggregations(
	aggs map[string]indexlib.Aggs,
) map[string]search.Aggregation {
	result := make(map[string]search.Aggregation, len(aggs))

	for name, agg := range aggs {
		if agg.Terms != nil {
			if agg.Terms.Size == 0 {
				agg.Terms.Size = 10
			}
			termsAggregation := aggregations.NewTermsAggregation(
				search.Field(agg.Terms.Field),
				agg.Terms.Size,
			)
			// nested aggregation (bucket aggregation need support)
			if len(agg.Aggs) > 0 {
				subAggs := b.generateAggregations(agg.Aggs)
				for k, v := range subAggs {
					termsAggregation.AddAggregation(k, v)
				}
			}
			result[name] = termsAggregation
		} else if agg.NumericRange != nil {
			ranges := aggregations.Ranges(search.Field(agg.NumericRange.Field))
			for _, value := range agg.NumericRange.Ranges {
				ranges.AddRange(aggregations.Range(value.From, value.To))
			}
			result[name] = ranges
		} else if agg.Sum != nil {
			result[name] = aggregations.Sum(search.Field(agg.Sum.Field))
		} else if agg.Min != nil {
			result[name] = aggregations.Min(search.Field(agg.Min.Field))
		} else if agg.Max != nil {
			result[name] = aggregations.Max(search.Field(agg.Max.Field))
		} else if agg.Avg != nil {
			result[name] = aggregations.Avg(search.Field(agg.Avg.Field))
		} else if agg.WeightedAvg != nil {
			result[name] = aggregations.WeightedAvg(search.Field(agg.WeightedAvg.Value.Field), search.Field(agg.WeightedAvg.Weight.Field))
		} else if agg.Cardinality != nil {
			result[name] = aggregations.Cardinality(search.Field(agg.Cardinality.Field))
		}
	}

	return result
}

func (b *BlugeReader) generateAggsResponse(
	buckets *search.Bucket,
) (map[string]indexlib.AggsResponse, error) {
	aggsResponse := make(map[string]indexlib.AggsResponse)
	for name, value := range buckets.Aggregations() {
		switch value := value.(type) {
		case search.BucketCalculator:
			aggsBuckets := make([]map[string]interface{}, 0)
			for _, bucket := range value.Buckets() {
				aggsBucket := make(map[string]interface{})
				aggsBucket["key"] = bucket.Name()
				aggsBucket["doc_count"] = bucket.Count()

				if bucket.Aggregations() != nil {
					aggsResponse, err := b.generateAggsResponse(bucket)
					if err != nil {
						return aggsResponse, err
					}
					for k, v := range aggsResponse {
						aggsBucket[k] = v
					}
				}
				aggsBuckets = append(aggsBuckets, aggsBucket)
			}
			aggsResponse[name] = indexlib.AggsResponse{Buckets: aggsBuckets}
		case search.MetricCalculator:
			aggsResponse[name] = indexlib.AggsResponse{Value: value.Value()}
		case search.DurationCalculator:
			aggsResponse[name] = indexlib.AggsResponse{Value: value.Duration().Milliseconds()}
		default:
			return aggsResponse, fmt.Errorf("bluge aggregation: %s calculator type: %s not support", name, value)
		}
	}
	return aggsResponse, nil
}

func (b *BlugeReader) Count() int {
	return len(b.Readers)
}

func (b *BlugeReader) Close() {
	for _, reader := range b.Readers {
		err := reader.Close()
		if err != nil {
			log.Printf("fail to close bluge reader for: %s", err)
		}
	}
}
