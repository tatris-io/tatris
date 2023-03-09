// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package bluge organizes codes of the indexing library bluge
package bluge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/tatris-io/tatris/internal/protocol"

	cfg "github.com/tatris-io/tatris/internal/core/config"

	"github.com/blugelabs/bluge/search/aggregations"
	"github.com/sourcegraph/conc/pool"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/common/utils"

	"github.com/blugelabs/bluge"
	"github.com/blugelabs/bluge/search"
	qs "github.com/blugelabs/query_string"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/indexlib"
	custom_aggregations "github.com/tatris-io/tatris/internal/indexlib/bluge/aggregations"
	"github.com/tatris-io/tatris/internal/indexlib/bluge/config"
)

type BlugeReader struct {
	*indexlib.BaseConfig
	Segments []string
	Readers  []*bluge.Reader
	// closeHook will be called in 'Close' func of BlugeReader if it is not nil
	closeHook func(*BlugeReader)
}

type BlugeSearchResult struct {
	docs    []*search.DocumentMatch
	buckets []*search.Bucket
}

var (
	errNotBlugeReader = errors.New("not a bluge reader")
)

func NewBlugeReader(
	config *indexlib.BaseConfig,
	segments ...string,
) *BlugeReader {
	return &BlugeReader{
		BaseConfig: config,
		Segments:   segments,
		Readers:    make([]*bluge.Reader, 0),
	}
}

func (b *BlugeReader) OpenReader() error {
	if len(b.Readers) > 0 {
		// opened
		return nil
	}

	var cfg bluge.Config

	for _, segment := range b.Segments {
		switch b.BaseConfig.StorageType {
		case indexlib.FSStorageType:
			cfg = config.GetFSConfig(b.BaseConfig.DataPath, segment)
		default:
			cfg = config.GetFSConfig(b.BaseConfig.DataPath, segment)
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
	limit, from int,
) (*indexlib.QueryResponse, error) {
	defer utils.Timerf(
		"bluge search docs finish, segments:%+v, query:%+v, limit:%d, from:%d",
		b.Segments,
		query,
		limit,
		from,
	)()
	p := pool.NewWithResults[*BlugeSearchResult]().WithErrors().
		WithMaxGoroutines(cfg.Cfg.Query.Parallel)
	for _, reader := range b.Readers {
		r := reader
		p.Go(func() (*BlugeSearchResult, error) {
			result := &BlugeSearchResult{
				docs:    make([]*search.DocumentMatch, 0),
				buckets: make([]*search.Bucket, 0),
			}
			searchRequest, err := b.generateSearchRequest(query, limit, from)
			if err != nil {
				return nil, err
			}
			dmi, err := r.Search(ctx, searchRequest)
			if err != nil {
				logger.Error(
					"bluge search failed",
					zap.Any("request", searchRequest),
					zap.Error(err),
				)
				return result, err
			}
			next, err := dmi.Next()
			for err == nil && next != nil {
				result.docs = append(result.docs, next)
				next, err = dmi.Next()
			}
			result.buckets = append(result.buckets, dmi.Aggregations())
			return result, nil
		})
	}

	results, err := p.Wait()
	if err != nil {
		return nil, err
	}
	// TODO: stream process the documents returned by multiple readers instead of loading them all
	// into memory instantaneously
	docs, bucket := b.dealMultiResults(results, generateSort(query), limit, from)

	// get buckets limit (map[aggName]size)
	bucketLimitDoc := make(map[string]int)
	if aggs := query.GetAggs(); aggs != nil {
		b.getBucketAggregationsLimit(aggs, bucketLimitDoc)
	}

	return b.generateResponse(docs, bucket, bucketLimitDoc)
}

func (b *BlugeReader) dealMultiResults(
	results []*BlugeSearchResult,
	sortOrder search.SortOrder,
	limit, from int,
) ([]*search.DocumentMatch, *search.Bucket) {
	docs := make([]*search.DocumentMatch, 0)
	var bucket *search.Bucket
	for _, result := range results {
		docs = append(docs, result.docs...)
		// merge bucket
		for _, b := range result.buckets {
			if bucket == nil {
				bucket = b
			} else {
				bucket.Merge(b)
			}
		}
	}
	// sort docs
	if sortOrder != nil && len(docs) > 1 {
		sort.SliceStable(docs, func(i, j int) bool {
			return sortOrder.Compare(docs[i], docs[j]) < 0
		})
	}

	// skip docs
	if from > 0 {
		if len(docs) <= from {
			docs = docs[:0]
		} else {
			docs = docs[from:]
		}
	}
	// limit docs
	if len(docs) > limit {
		docs = docs[:limit]
	}
	return docs, bucket
}

func (b *BlugeReader) generateSearchRequest(
	query indexlib.QueryRequest,
	limit, from int,
) (bluge.SearchRequest, error) {
	blugeQuery, err := b.generateQuery(query)
	if err != nil {
		return nil, err
	}
	size := limit + from
	searchRequest := bluge.NewTopNSearch(size, blugeQuery).WithStandardAggregations()
	sorts := generateSort(query)
	if sorts != nil {
		searchRequest.SortByCustom(sorts)
	}
	if aggs := query.GetAggs(); aggs != nil {
		blugeAggs, err := b.generateAggregations(aggs)
		if err != nil {
			return nil, err
		}
		for name, agg := range blugeAggs {
			searchRequest.AddAggregation(name, agg)
		}
	}
	return searchRequest, nil
}

func generateSort(query indexlib.QueryRequest) []*search.Sort {
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
		return sorts
	}
	return nil
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
		q, err := ParseRangeQuery(query)
		if err != nil {
			return nil, err
		}
		blugeQuery = q
	}

	return blugeQuery, nil
}

func (b *BlugeReader) generateResponse(
	docs []*search.DocumentMatch,
	bucket *search.Bucket,
	bucketLimitDoc map[string]int,
) (*indexlib.QueryResponse, error) {

	Hits := make([]indexlib.Hit, 0)
	for _, doc := range docs {
		var id string
		var index string
		var source protocol.Document
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

	bucket.Aggregation("duration").Finish()

	aggsResponse, err := b.generateAggsResponse(bucket, bucketLimitDoc)
	if err != nil {
		return nil, err
	}

	resp := &indexlib.QueryResponse{
		Took: bucket.Duration().Milliseconds(),
		Hits: indexlib.Hits{
			Total: indexlib.Total{Value: int64(bucket.Count())},
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
	analyzer := generateAnalyzer(query.Analyzer)
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
	analyzer := generateAnalyzer(query.Analyzer)
	if analyzer != nil {
		options.WithDefaultAnalyzer(analyzer)
	}

	return qs.ParseQueryString(query.Query, options)
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

func (b *BlugeReader) getBucketAggregationsLimit(
	aggs map[string]indexlib.Aggs,
	bucketLimitDoc map[string]int,
) {
	for name, agg := range aggs {
		if agg.Terms != nil {
			bucketLimitDoc[name] = agg.Terms.Size
		}
	}
}

func (b *BlugeReader) generateAggregations(
	aggs map[string]indexlib.Aggs,
) (map[string]search.Aggregation, error) {
	result := make(map[string]search.Aggregation, len(aggs))

	for name, agg := range aggs {
		if agg.Terms != nil {
			termsAggregation := aggregations.NewTermsAggregation(
				search.Field(agg.Terms.Field),
				agg.Terms.ShardSize,
			)
			// sub-aggregations (bucket aggregation need support)
			if len(agg.Aggs) > 0 {
				subAggs, err := b.generateAggregations(agg.Aggs)
				if err != nil {
					return nil, err
				}
				for k, v := range subAggs {
					termsAggregation.AddAggregation(k, v)
				}
			}
			result[name] = termsAggregation
		} else if agg.Filter != nil {
			filter, err := b.generateAggsFilter(agg.Filter.FilterQuery, agg.Aggs)
			if err != nil {
				return nil, err
			}
			result[name] = filter
		} else if d := agg.DateHistogram; d != nil {
			dateHistogramAggregation := custom_aggregations.NewDateHistogramAggregation(
				search.Field(d.Field), d.CalendarInterval,
				d.FixedInterval, d.Format, d.TimeZone, d.Offset,
				d.MinDocCount, d.ExtendedBounds, d.HardBounds,
			)
			// sub-aggregations (bucket aggregation need support)
			if len(agg.Aggs) > 0 {
				subAggs, err := b.generateAggregations(agg.Aggs)
				if err != nil {
					return nil, err
				}
				for k, v := range subAggs {
					dateHistogramAggregation.AddAggregation(k, v)
				}
			}
			result[name] = dateHistogramAggregation
		} else if d := agg.Histogram; d != nil {
			histogramAggregation := custom_aggregations.NewHistogramAggregation(
				search.Field(d.Field),
				d.Interval,
				d.Offset,
				d.MinDocCount,
				d.ExtendedBounds,
				d.HardBounds,
			)
			// when executing a histogram aggregation over a histogram field, no sub-aggregations
			// are allowed.
			result[name] = histogramAggregation
		} else if agg.NumericRange != nil {
			ranges := aggregations.Ranges(search.Field(agg.NumericRange.Field))
			for _, value := range agg.NumericRange.Ranges {
				ranges.AddRange(aggregations.Range(value.From, value.To))
			}
			// sub-aggregations (bucket aggregation need support)
			if len(agg.Aggs) > 0 {
				subAggs, err := b.generateAggregations(agg.Aggs)
				if err != nil {
					return nil, err
				}
				for k, v := range subAggs {
					ranges.AddAggregation(k, v)
				}
			}
			result[name] = ranges
		} else if agg.DateRange != nil {
			ranges := aggregations.DateRanges(search.Field(agg.DateRange.Field))
			var fromValue any
			var toValue any
			var err error
			for _, value := range agg.DateRange.Ranges {
				fromValue, err = strconv.ParseInt(value.From, 10, 64)
				if err != nil {
					fromValue = value.From
				}
				from, err := utils.ParseTime(fromValue)
				if err != nil {
					return nil, &errs.InvalidFieldValError{Field: "range", Type: "date", Value: fromValue}
				}

				toValue, err = strconv.ParseInt(value.To, 10, 64)
				if err != nil {
					toValue = value.To
				}
				to, err := utils.ParseTime(toValue)
				if err != nil {
					return nil, &errs.InvalidFieldValError{Field: "range", Type: "date", Value: fromValue}
				}

				if timeZone := agg.DateRange.TimeZone; timeZone != nil {
					from = from.In(timeZone)
					to = to.In(timeZone)
				}
				ranges.AddRange(aggregations.NewDateRange(from, to))
			}
			// sub-aggregations (bucket aggregation need support)
			if len(agg.Aggs) > 0 {
				subAggs, err := b.generateAggregations(agg.Aggs)
				if err != nil {
					return nil, err
				}
				for k, v := range subAggs {
					ranges.AddAggregation(k, v)
				}
			}
			result[name] = ranges
		} else if agg.Count != nil {
			result[name] = aggregations.CountMatches()
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
		} else if agg.Percentiles != nil {
			result[name] = custom_aggregations.NewPercentiles(search.Field(agg.Percentiles.Field), agg.Percentiles.Percents, agg.Percentiles.Compression)
		}
	}

	return result, nil
}

func (b *BlugeReader) generateAggsFilter(
	query indexlib.QueryRequest,
	aggs map[string]indexlib.Aggs,
) (search.Aggregation, error) {
	var filterAggs search.Aggregation

	switch query := query.(type) {
	case *indexlib.TermQuery:
		termsAggregation := aggregations.NewTermsAggregation(
			aggregations.FilterText(search.Field(query.Field),
				func(bytes []byte) bool {
					return string(bytes) == query.Term
				}),
			cfg.Cfg.Query.DefaultAggregationShardSize,
		)
		filterAggs = termsAggregation
	case *indexlib.RangeQuery:
		rangeQuery, err := ParseAggsFilterRangeQuery(query)
		if err != nil {
			return nil, err
		}
		filterAggs = rangeQuery
	default:
		return nil, fmt.Errorf("query type [%s] is not supported for filter aggregation", query)
	}

	// sub-aggregations
	if len(aggs) > 0 {
		subAggs, err := b.generateAggregations(aggs)
		if err != nil {
			return nil, err
		}
		for k, v := range subAggs {
			switch filterAggs := filterAggs.(type) {
			case *aggregations.TermsAggregation:
				filterAggs.AddAggregation(k, v)
			case *aggregations.RangeAggregation:
				filterAggs.AddAggregation(k, v)
			case *aggregations.DateRangeAggregation:
				filterAggs.AddAggregation(k, v)
			default:
				return nil, fmt.Errorf("query type [%s] is not supported for filter aggregation", query)
			}
		}
	}

	return filterAggs, nil
}

func (b *BlugeReader) generateAggsResponse(
	bucket *search.Bucket,
	bucketLimitDoc map[string]int,
) (map[string]indexlib.Aggregation, error) {
	aggsResponse := make(map[string]indexlib.Aggregation)
	for name, value := range bucket.Aggregations() {
		switch value := value.(type) {
		case search.BucketCalculator:
			aggsBuckets := make([]protocol.Bucket, 0)
			buckets := value.Buckets()
			count := len(buckets)
			// limit bucket result
			if limit, ok := bucketLimitDoc[name]; ok && limit < count {
				count = limit
			}

			for i := 0; i < count; i++ {
				aggsBucket := make(map[string]interface{})
				aggsBucket["key"] = buckets[i].Name()
				aggsBucket["doc_count"] = buckets[i].Count()

				if buckets[i].Aggregations() != nil {
					aggsResponse, err := b.generateAggsResponse(buckets[i], bucketLimitDoc)
					if err != nil {
						return aggsResponse, err
					}
					for k, v := range aggsResponse {
						aggsBucket[k] = v
					}
				}
				aggsBuckets = append(aggsBuckets, aggsBucket)
			}
			aggsResponse[name] = indexlib.Aggregation{Type: consts.AggregationTypeBucket, Buckets: aggsBuckets}
		case search.MetricCalculator:
			aggsResponse[name] = indexlib.Aggregation{Type: consts.AggregationTypeMetric, Value: value.Value()}
		case *custom_aggregations.PercentilesCalculator:
			aggsResponse[name] = indexlib.Aggregation{Type: consts.AggregationTypePercentile, Value: value.Value()}
		case search.DurationCalculator:
			aggsResponse[name] = indexlib.Aggregation{Type: consts.AggregationTypeDuration, Value: value.Duration().Milliseconds()}
		default:
			return aggsResponse, &errs.UnsupportedError{Desc: "aggregation calculator", Value: value}
		}
	}
	return aggsResponse, nil
}

func (b *BlugeReader) Count() int {
	return len(b.Readers)
}

func (b *BlugeReader) Close() {
	if b.closeHook != nil {
		b.closeHook(b)
		return
	}

	for _, reader := range b.Readers {
		err := reader.Close()
		if err != nil {
			log.Printf("fail to close bluge reader for: %s", err)
		}
	}
}

// MergeReader multiple readers into one BlugeReader.
// The readers(or their underlying readers) must be type of BlugeReader.
func MergeReader(config *indexlib.BaseConfig, readers ...indexlib.Reader) (*BlugeReader, error) {
	// 99% case readers has 1 index and 1 reader, so the slice capacity is set to len(readers)
	blugeReaders := make([]*bluge.Reader, 0, len(readers))

	for _, reader := range readers {
		unwrap := indexlib.UnwrapReader(reader)
		if unwrap == nil {
			return nil, errNotBlugeReader
		}
		converted, ok := unwrap.(*BlugeReader)
		if !ok {
			return nil, errNotBlugeReader
		}
		blugeReaders = append(blugeReaders, converted.Readers...)
	}

	return &BlugeReader{
		BaseConfig: config,
		Readers:    blugeReaders,
		closeHook: func(_ *BlugeReader) {
			for _, reader := range readers {
				reader.Close()
			}
		},
	}, nil
}
