// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package bluge organizes codes of the indexing library bluge
package bluge

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/tatris-io/tatris/internal/protocol"

	cfg "github.com/tatris-io/tatris/internal/core/config"

	"github.com/blugelabs/bluge/search/aggregations"
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
	*indexlib.Config
	Segments []string
	Readers  []*bluge.Reader
	// closeHook will be called in 'Close' func of BlugeReader if it is not nil
	closeHook func(*BlugeReader)
}

type ReaderResult struct {
	docs   []*search.DocumentMatch
	bucket *search.Bucket
}

var (
	errNotBlugeReader = errors.New("not a bluge reader")

	tokens chan struct{}
)

func init() {
	tokenLimit := cfg.Cfg.Query.GlobalReadersLimit
	tokens = make(chan struct{}, tokenLimit)
	go func() {
		ticker := time.NewTicker(time.Second)
		for range ticker.C {
			logger.Info(
				"global available tokens",
				zap.Int("count", tokenLimit-len(tokens)),
			)
		}
	}()
}

func NewBlugeReader(
	config *indexlib.Config,
	segments ...string,
) *BlugeReader {
	return &BlugeReader{
		Config:   config,
		Segments: segments,
		Readers:  make([]*bluge.Reader, 0),
	}
}

func (b *BlugeReader) OpenReader() error {
	if len(b.Readers) > 0 {
		// opened
		return nil
	}

	var cfg bluge.Config

	for _, segment := range b.Segments {
		switch b.Config.DirectoryType {
		case consts.DirectoryFS:
			cfg = config.GetFSConfig(b.Config.FS.Path, segment)
		case consts.DirectoryOSS:
			cfg = config.GetOSSConfig(
				b.Config.OSS.Endpoint,
				b.Config.OSS.Bucket,
				b.Config.OSS.AccessKeyID,
				b.Config.OSS.SecretAccessKey,
				segment,
				b.Config.OSS.MinimumConcurrencyLoadSize,
			)
		default:
			cfg = config.GetFSConfig(b.Config.FS.Path, segment)
		}

		reader, err := bluge.OpenReader(cfg)
		if err != nil {
			for _, r := range b.Readers {
				r.Close()
			}
			b.Readers = nil
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

	documents := &DocHeap{docs: make([]*search.DocumentMatch, 0), sort: genSort(query)}
	heap.Init(documents)
	aggregation := search.NewBucket("aggregation", nil)

	resultChan := make(chan *ReaderResult, len(b.Readers))
	errChan := make(chan error, len(b.Readers))

	var loading, merging sync.WaitGroup

	// load data from multiple readers in parallel, which is limited by global tokens
	for _, reader := range b.Readers {
		loading.Add(1)
		r := reader
		go func() {
			if err := b.load(ctx, r, query, resultChan, &loading, limit, from); err != nil {
				logger.Error(
					"bluge search failed",
					zap.Any("query", query),
					zap.Error(err),
				)
				<-tokens
				errChan <- err
			}
		}()
	}

	// control channel closure
	go func() {
		loading.Wait()
		close(resultChan)
		close(errChan)
	}()

	// stream merge data loaded from multiple readers above
	merging.Add(1)
	go b.merge(documents, aggregation, resultChan, &merging)

	merging.Wait()

	for err := range errChan {
		return nil, err
	}

	// collate the documents
	collatedDocs := b.collate(documents, limit, from)

	bucketLimitDoc := make(map[string]int)
	if aggs := query.GetAggs(); aggs != nil {
		b.getBucketAggregationsLimit(aggs, bucketLimitDoc)
	}

	return b.response(collatedDocs, aggregation, bucketLimitDoc)
}

func (b *BlugeReader) load(
	ctx context.Context,
	r *bluge.Reader,
	query indexlib.QueryRequest,
	resultChan chan *ReaderResult,
	wg *sync.WaitGroup,
	limit, from int,
) error {
	defer wg.Done()
	tokens <- struct{}{}
	req, err := b.request(query, limit, from)
	if err != nil {
		return err
	}
	dmi, err := r.Search(ctx, req)
	if err != nil {
		return err
	}
	docs := make([]*search.DocumentMatch, 0)
	next, err := dmi.Next()
	for err == nil && next != nil {
		docs = append(docs, next)
		next, err = dmi.Next()
	}
	searchResult := &ReaderResult{docs: docs, bucket: dmi.Aggregations()}
	resultChan <- searchResult
	return nil
}

func (b *BlugeReader) merge(
	docs *DocHeap,
	bucket *search.Bucket,
	resultChan chan *ReaderResult,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for result := range resultChan {
		doMerge(docs, bucket, result)
	}
}

func doMerge(docs *DocHeap, bucket *search.Bucket, result *ReaderResult) {
	defer func() {
		<-tokens
	}()
	for _, doc := range result.docs {
		heap.Push(docs, doc)
	}
	bucket.Merge(result.bucket)
}

func (b *BlugeReader) collate(
	docs *DocHeap,
	limit, from int,
) []*search.DocumentMatch {
	docsRes := make([]*search.DocumentMatch, 0)
	if from >= 0 {
		// skip docs
		for from > 0 && docs.Len() > 0 {
			heap.Pop(docs)
			from--
		}
		// limit docs
		for limit > 0 && docs.Len() > 0 {
			docsRes = append(docsRes, heap.Pop(docs).(*search.DocumentMatch))
			limit--
		}
		// sort docs
		if docs.sort != nil {
			sort.SliceStable(docsRes, func(i, j int) bool {
				return docs.sort.Compare(docsRes[i], docsRes[j]) < 0
			})
		}
	}
	return docsRes
}

func (b *BlugeReader) request(
	query indexlib.QueryRequest,
	limit, from int,
) (bluge.SearchRequest, error) {
	blugeQuery, err := b.genQuery(query)
	if err != nil {
		return nil, err
	}
	size := limit + from
	searchRequest := bluge.NewTopNSearch(size, blugeQuery).WithStandardAggregations()
	sorts := genSort(query)
	if sorts != nil {
		searchRequest.SortByCustom(sorts)
	}
	if aggs := query.GetAggs(); aggs != nil {
		blugeAggs, err := b.genAggregations(aggs)
		if err != nil {
			return nil, err
		}
		for name, agg := range blugeAggs {
			searchRequest.AddAggregation(name, agg)
		}
	}
	return searchRequest, nil
}

func genSort(query indexlib.QueryRequest) []*search.Sort {
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

func (b *BlugeReader) genQuery(query indexlib.QueryRequest) (bluge.Query, error) {
	var blugeQuery bluge.Query

	switch query := query.(type) {
	case *indexlib.MatchAllQuery:
		q := bluge.NewMatchAllQuery()
		blugeQuery = q
	case *indexlib.MatchQuery:
		q, err := b.genMatchQuery(query)
		if err != nil {
			return nil, err
		}
		blugeQuery = q
	case *indexlib.MatchPhraseQuery:
		q, err := b.genMatchPhraseQuery(query)
		if err != nil {
			return nil, err
		}
		blugeQuery = q
	case *indexlib.QueryString:
		q, err := b.genQueryString(query)
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
		q, err := b.genBoolQuery(query)
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

func (b *BlugeReader) response(
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
			Type:      "_doc",
			Score:     doc.Score}
		Hits = append(Hits, hit)
	}

	bucket.Aggregation("duration").Finish()

	aggsResponse, err := b.genAggsResponse(bucket, bucketLimitDoc)
	if err != nil {
		return nil, err
	}

	resp := &indexlib.QueryResponse{
		Took: bucket.Duration().Milliseconds(),
		Hits: indexlib.Hits{
			Total:    indexlib.Total{Value: int64(bucket.Count()), Relation: "eq"},
			Hits:     Hits,
			MaxScore: bucket.Metric("max_score"),
		},
		Aggregations: aggsResponse,
	}

	return resp, nil
}

func (b *BlugeReader) genMatchQuery(query *indexlib.MatchQuery) (bluge.Query, error) {
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
	analyzer := genAnalyzer(query.Analyzer)
	if analyzer != nil {
		q.SetAnalyzer(analyzer)
	}

	return q, nil
}

func (b *BlugeReader) genMatchPhraseQuery(
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

func (b *BlugeReader) genQueryString(query *indexlib.QueryString) (bluge.Query, error) {
	options := qs.DefaultOptions()
	analyzer := genAnalyzer(query.Analyzer)
	if analyzer != nil {
		options.WithDefaultAnalyzer(analyzer)
	}

	return qs.ParseQueryString(query.Query, options)
}

func (b *BlugeReader) genBoolQuery(query *indexlib.BooleanQuery) (bluge.Query, error) {
	q := bluge.NewBooleanQuery()
	if query.Musts != nil {
		for _, must := range query.Musts {
			tmpQuery, err := b.genQuery(must)
			if err != nil {
				return nil, err
			}
			q.AddMust(tmpQuery)
		}
	}
	if query.MustNots != nil {
		for _, mustNot := range query.MustNots {
			tmpQuery, err := b.genQuery(mustNot)
			if err != nil {
				return nil, err
			}
			q.AddMustNot(tmpQuery)
		}
	}
	if query.Shoulds != nil {
		for _, should := range query.Shoulds {
			tmpQuery, err := b.genQuery(should)
			if err != nil {
				return nil, err
			}
			q.AddShould(tmpQuery)
		}
	}
	if query.Filters != nil {
		filter := bluge.NewBooleanQuery().SetBoost(0)
		for _, fliter := range query.Filters {
			tmpQuery, err := b.genQuery(fliter)
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

func (b *BlugeReader) genAggregations(
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
				subAggs, err := b.genAggregations(agg.Aggs)
				if err != nil {
					return nil, err
				}
				for k, v := range subAggs {
					termsAggregation.AddAggregation(k, v)
				}
			}
			result[name] = termsAggregation
		} else if agg.Filter != nil {
			filter, err := b.genAggsFilter(agg.Filter.FilterQuery, agg.Aggs)
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
				subAggs, err := b.genAggregations(agg.Aggs)
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
				subAggs, err := b.genAggregations(agg.Aggs)
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
				subAggs, err := b.genAggregations(agg.Aggs)
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

func (b *BlugeReader) genAggsFilter(
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
		subAggs, err := b.genAggregations(aggs)
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

func (b *BlugeReader) genAggsResponse(
	bucket *search.Bucket,
	bucketLimitDoc map[string]int,
) (map[string]indexlib.Aggregation, error) {
	aggsResponse := make(map[string]indexlib.Aggregation)
	for name, value := range bucket.Aggregations() {
		// Skip the following fields to be compatible with the elasticsearch protocol, otherwise,
		// users using the elasticsearch SDK will get an error like:
		// "Could not parse aggregation keyed as [...]"
		if name == "count" || name == "duration" || name == "max_score" {
			continue
		}
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
					aggsResponse, err := b.genAggsResponse(buckets[i], bucketLimitDoc)
					if err != nil {
						return aggsResponse, err
					}
					for k, v := range aggsResponse {
						aggsBucket[k] = v
					}
				}
				aggsBuckets = append(aggsBuckets, aggsBucket)
			}
			aggsResponse[name] = indexlib.Aggregation{Buckets: aggsBuckets}
		case search.MetricCalculator:
			aggsResponse[name] = indexlib.Aggregation{Value: value.Value()}
		case *custom_aggregations.PercentilesCalculator:
			aggsResponse[name] = indexlib.Aggregation{Value: value.Value()}
		case search.DurationCalculator:
			aggsResponse[name] = indexlib.Aggregation{Value: value.Duration().Milliseconds()}
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
func MergeReader(
	config *indexlib.Config,
	segments []string,
	readers []indexlib.Reader,
) (*BlugeReader, error) {
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
		Config:   config,
		Segments: segments,
		Readers:  blugeReaders,
		closeHook: func(_ *BlugeReader) {
			for _, reader := range readers {
				reader.Close()
			}
		},
	}, nil
}
