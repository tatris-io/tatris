// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package query organizes codes on the query routine
package query

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/tatris-io/tatris/internal/common/utils"
	"github.com/xhit/go-str2duration/v2"

	"github.com/tatris-io/tatris/internal/core"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/protocol"
	"go.uber.org/zap"
)

func SearchDocs(
	indexes []*core.Index,
	request protocol.QueryRequest,
) (*protocol.QueryResponse, error) {
	start, end, err := timeRange(request.Query)
	if err != nil {
		return nil, err
	}
	var allSegments []*core.Segment
	for _, index := range indexes {
		segments := index.GetSegmentsByTime(start, end)
		allSegments = append(allSegments, segments...)
	}
	if len(allSegments) == 0 {
		// no match any segments, returns an appropriate response
		return &protocol.QueryResponse{Hits: protocol.Hits{Hits: []protocol.Hit{}}}, nil
	}
	reader, err := core.MergeSegmentReader(&indexlib.BaseConfig{
		DataPath: consts.DefaultDataPath,
	}, allSegments...)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	hits := protocol.Hits{
		Total: protocol.Total{Value: 0, Relation: "eq"},
	}
	// use the mappings of the first index to transform query
	libRequest, err := transform(request.Query, indexes[0].Mappings)
	if err != nil {
		return nil, err
	}
	if aggs := request.Aggs; aggs != nil {
		agg, err := transformAggs(aggs)
		if err != nil {
			return nil, err
		}
		libRequest.SetAggs(agg)
	}
	if sort := request.Sort; sort != nil {
		libRequest.SetSort(transformSort(sort))
	}

	hits.Hits = make([]protocol.Hit, 0)
	aggregations := make(map[string]protocol.AggsResponse)

	resp, err := reader.Search(context.Background(), libRequest, int(request.Size))
	if err != nil {
		return nil, err
	}
	respHits := resp.Hits
	for _, respHit := range respHits.Hits {
		hits.Hits = append(
			hits.Hits,
			protocol.Hit{Index: respHit.Index, ID: respHit.ID, Source: respHit.Source},
		)
	}

	for k, v := range resp.Aggregations {
		aggregations[k] = protocol.AggsResponse{Value: v.Value, Buckets: v.Buckets}
	}

	hits.Total.Value = respHits.Total.Value
	hits.Total.Relation = respHits.Total.Relation
	return &protocol.QueryResponse{Hits: hits, Aggregations: aggregations}, nil
}

func timeRange(query protocol.Query) (int64, int64, error) {
	// default range: last 3 days
	start, end := time.Now().UnixMilli()-60000*60*24*3, time.Now().UnixMilli()
	if query.Range != nil {
		timeRange, ok := query.Range[consts.TimestampField]
		if ok {
			if timeRange.Gt != nil {
				start = timeRange.Gt.(time.Time).UnixMilli() + 1
			}
			if timeRange.Gte != nil {
				start = timeRange.Gte.(time.Time).UnixMilli()
			}
			if timeRange.Lt != nil {
				end = timeRange.Lt.(time.Time).UnixMilli() - 1
			}
			if timeRange.Lte != nil {
				end = timeRange.Lte.(time.Time).UnixMilli()
			}
		}
	} else if query.Bool != nil {
		// TODO
		logger.Warn("unsupported: extract timeRange from bool query", zap.String("role", "query"))
	}
	if start > end {
		return start, end, &errs.InvalidQueryError{Query: query, Message: "invalid time range"}
	}
	return start, end, nil
}

func transform(query protocol.Query, mappings *protocol.Mappings) (indexlib.QueryRequest, error) {
	if query.MatchAll != nil {
		return transformMatchAll()
	} else if query.Match != nil {
		return transformMatch(query, mappings)
	} else if query.MatchPhrase != nil {
		return transformMatchPhrase(query)
	} else if query.QueryString != nil {
		return transformQueryString(query)
	} else if query.Term != nil {
		return transformTerm(query)
	} else if query.Ids != nil {
		return transformIds(query)
	} else if query.Terms != nil {
		return transformTerms(query)
	} else if query.Range != nil {
		return transformRange(query)
	} else if query.Bool != nil {
		return transformBool(query, mappings)
	} else {
		// Exposed queries allow users to specify no query, example: "{"size": xx}"
		// Use match all query when query is nil
		return transformMatchAll()
	}
}

func transformAggs(aggs map[string]protocol.Aggs) (map[string]indexlib.Aggs, error) {
	result := make(map[string]indexlib.Aggs, len(aggs))

	for name, agg := range aggs {
		indexlibAggs := &indexlib.Aggs{}
		if agg.Terms != nil {
			if agg.Terms.Field == "" {
				return nil, errs.ErrEmptyField
			}
			if agg.Terms.Size == 0 {
				agg.Terms.Size = 10
			}
			if agg.Terms.ShardSize == 0 {
				agg.Terms.ShardSize = 5000
			}

			indexlibAggs.Terms = &indexlib.AggTerms{
				Field:     agg.Terms.Field,
				Size:      agg.Terms.Size,
				ShardSize: agg.Terms.ShardSize,
			}
		} else if agg.NumericRange != nil {
			if agg.NumericRange.Field == "" {
				return nil, errs.ErrEmptyField
			}
			indexLibRanges := make([]indexlib.NumericRange, 0, len(agg.NumericRange.Ranges))
			if ranges := agg.NumericRange.Ranges; ranges != nil {
				for _, r := range ranges {
					indexLibRanges = append(indexLibRanges, indexlib.NumericRange{From: r.From, To: r.To})
				}
			}
			indexlibAggs.NumericRange = &indexlib.AggNumericRange{Field: agg.NumericRange.Field, Ranges: indexLibRanges, Keyed: agg.NumericRange.Keyed}
		} else if agg.Sum != nil {
			if agg.Sum.Field == "" {
				return nil, errs.ErrEmptyField
			}
			indexlibAggs.Sum = &indexlib.AggMetric{Field: agg.Sum.Field}
		} else if agg.Min != nil {
			if agg.Min.Field == "" {
				return nil, errs.ErrEmptyField
			}
			indexlibAggs.Min = &indexlib.AggMetric{Field: agg.Min.Field}
		} else if agg.Max != nil {
			if agg.Max.Field == "" {
				return nil, errs.ErrEmptyField
			}
			indexlibAggs.Max = &indexlib.AggMetric{Field: agg.Max.Field}
		} else if agg.Avg != nil {
			if agg.Avg.Field == "" {
				return nil, errs.ErrEmptyField
			}
			indexlibAggs.Avg = &indexlib.AggMetric{Field: agg.Avg.Field}
		} else if agg.WeightedAvg != nil {
			if agg.WeightedAvg.Value.Field == "" {
				return nil, errs.ErrEmptyField
			}
			indexlibAggs.WeightedAvg = &indexlib.AggWeightedAvg{
				Value:  &indexlib.AggMetric{Field: agg.WeightedAvg.Value.Field},
				Weight: &indexlib.AggMetric{Field: agg.WeightedAvg.Weight.Field},
			}
		} else if agg.Cardinality != nil {
			if agg.Cardinality.Field == "" {
				return nil, errs.ErrEmptyField
			}
			indexlibAggs.Cardinality = &indexlib.AggMetric{Field: agg.Cardinality.Field}
		} else if agg.DateHistogram != nil {
			err := transformDateHistogramAgg(agg.DateHistogram, indexlibAggs)
			if err != nil {
				return nil, err
			}
		}

		// nested aggs
		if agg.Aggs != nil {
			var err error
			indexlibAggs.Aggs, err = transformAggs(agg.Aggs)
			if err != nil {
				return nil, err
			}
		}

		result[name] = *indexlibAggs
	}

	return result, nil
}

func transformSort(sort protocol.Sort) indexlib.Sort {
	result := make([]map[string]indexlib.SortTerm, len(sort))
	for i, s := range sort {
		result[i] = make(map[string]indexlib.SortTerm)
		for k, v := range s {
			result[i][k] = indexlib.SortTerm{Order: v.Order, Missing: v.Missing}
		}
	}
	return result
}

func transformMatchAll() (indexlib.QueryRequest, error) {
	return indexlib.NewMatchAllQuery(), nil
}

func transformMatch(
	query protocol.Query,
	mappings *protocol.Mappings,
) (indexlib.QueryRequest, error) {
	matches := query.Match
	if len(matches) <= 0 {
		return nil, &errs.InvalidQueryError{Query: query, Message: "invalid match"}
	}
	matchQ := indexlib.NewMatchQuery()
	for k, v := range matches {
		matchQ.Field = k
		switch v := v.(type) {
		case string:
			matchQ.Match = v
		case map[string]interface{}:
			matchQ.Match = v["query"].(string)
			if operator, ok := v["operator"]; ok {
				matchQ.Operator = operator.(string)
			}
			if fuzziness, ok := v["fuzziness"]; ok {
				matchQ.Fuzziness = int(fuzziness.(float64))
			}
			if prefix, ok := v["prefix_length"]; ok {
				matchQ.Prefix = int(prefix.(float64))
			}
			if analyzer, ok := v["analyzer"]; ok {
				matchQ.Analyzer = analyzer.(string)
			}
		}
	}
	// The match query does not match when the bluge keyword field value contains uppercase letters
	// Set KEYWORD analyzer
	if mappings.Properties[matchQ.Field].Type == consts.LibFieldTypeKeyword {
		matchQ.Analyzer = "KEYWORD"
	}
	return matchQ, nil
}

func transformMatchPhrase(query protocol.Query) (indexlib.QueryRequest, error) {
	matches := query.MatchPhrase
	if len(matches) <= 0 {
		return nil, &errs.InvalidQueryError{Query: query, Message: "invalid match_phrase"}
	}
	matchQ := indexlib.NewMatchPhraseQuery()
	for k, v := range matches {
		matchQ.Field = k
		switch v := v.(type) {
		case string:
			matchQ.MatchPhrase = v
		case map[string]interface{}:
			matchQ.MatchPhrase = v["query"].(string)
			if slop, ok := v["slop"]; ok {
				matchQ.Slop = int(slop.(float64))
			}
			if analyzer, ok := v["analyzer"]; ok {
				matchQ.Analyzer = analyzer.(string)
			}
		}
	}
	return matchQ, nil
}

func transformQueryString(query protocol.Query) (indexlib.QueryRequest, error) {
	querys := query.QueryString
	if len(querys) <= 0 {
		return nil, &errs.InvalidQueryError{Query: query, Message: "invalid query_string"}
	}
	queryStr := indexlib.NewQueryString()
	queryStr.Query = querys["query"].(string)
	if analyzer, ok := querys["analyzer"]; ok {
		queryStr.Analyzer = analyzer.(string)
	}

	return queryStr, nil
}

func transformTerm(query protocol.Query) (indexlib.QueryRequest, error) {
	term := query.Term
	if len(term) <= 0 {
		return indexlib.NewTermQuery(), nil
	}
	termQ := indexlib.NewTermQuery()
	for k, v := range term {
		termQ.Field = k
		switch v := v.(type) {
		case string:
			termQ.Term = v
		case map[string]interface{}:
			termQ.Term = v["value"].(string)
		}
	}
	return termQ, nil
}

func transformIds(query protocol.Query) (indexlib.QueryRequest, error) {
	ids := *query.Ids
	if len(ids.Values) <= 0 {
		return &indexlib.Terms{}, nil
	}
	termsQ := indexlib.NewTerms()
	termsQ.Fields = append(termsQ.Fields, ids.Values...)
	return termsQ, nil
}

func transformTerms(query protocol.Query) (indexlib.QueryRequest, error) {
	terms := query.Terms
	if len(terms) <= 0 {
		return &indexlib.TermsQuery{}, nil
	}
	field := ""
	values := []string{}
	termsQ := indexlib.NewTermsQuery()
	for k, v := range terms {
		field = k
		for _, vv := range v {
			switch vv := vv.(type) {
			case string:
				values = append(values, vv)
			default:
				return nil, &errs.UnsupportedError{Desc: "term", Value: vv}
			}
		}
		termsQ.Terms[field] = &indexlib.Terms{
			Fields: values,
		}
	}
	return termsQ, nil
}

func transformRange(query protocol.Query) (indexlib.QueryRequest, error) {
	rangeQuery := query.Range
	if len(rangeQuery) <= 0 {
		return &indexlib.RangeQuery{}, nil
	}

	rangeQ := indexlib.NewRangeQuery()
	for k, v := range rangeQuery {
		rangeQ.Range = map[string]*indexlib.RangeVal{
			k: {
				GT:  v.Gt,
				GTE: v.Gte,
				LT:  v.Lt,
				LTE: v.Lte,
			},
		}
	}
	return rangeQ, nil
}

func transformBool(
	query protocol.Query,
	mappings *protocol.Mappings,
) (indexlib.QueryRequest, error) {
	q := indexlib.NewBooleanQuery()

	if query.Bool.Must != nil {
		q.Musts = make([]indexlib.QueryRequest, 0, len(query.Bool.Must))
		for _, must := range query.Bool.Must {
			queryRequest, err := transform(*must, mappings)
			if err != nil {
				return nil, err
			}
			q.Musts = append(q.Musts, queryRequest)
		}
	}
	if query.Bool.MustNot != nil {
		q.MustNots = make([]indexlib.QueryRequest, 0, len(query.Bool.MustNot))
		for _, mustNot := range query.Bool.MustNot {
			queryRequest, err := transform(*mustNot, mappings)
			if err != nil {
				return nil, err
			}
			q.MustNots = append(q.MustNots, queryRequest)
		}
	}
	if query.Bool.Should != nil {
		q.Shoulds = make([]indexlib.QueryRequest, 0, len(query.Bool.Should))
		for _, should := range query.Bool.Should {
			queryRequest, err := transform(*should, mappings)
			if err != nil {
				return nil, err
			}
			q.Shoulds = append(q.Shoulds, queryRequest)
		}
	}
	if query.Bool.Filter != nil {
		q.Filters = make([]indexlib.QueryRequest, 0, len(query.Bool.Filter))
		for _, filter := range query.Bool.Filter {
			queryRequest, err := transform(*filter, mappings)
			if err != nil {
				return nil, err
			}
			q.Filters = append(q.Filters, queryRequest)
		}
	}
	if query.Bool.MinimumShouldMatch != "" {
		minShould, err := strconv.Atoi(query.Bool.MinimumShouldMatch)
		if err != nil {
			return nil, err
		}
		q.MinShould = minShould
	}
	return q, nil
}

func transformDateHistogramAgg(d *protocol.AggDateHistogram, indexlibAggs *indexlib.Aggs) error {
	if d.Field == "" {
		return errs.ErrEmptyField
	}
	if d.Interval != "" && d.FixedInterval == "" {
		d.FixedInterval = d.Interval
	}
	if d.FixedInterval == "" && d.CalendarInterval == "" {
		return errors.New(
			"required one of fields [fixed_interval, calendar_interval], but none were specified",
		)
	}
	var fixedInterval time.Duration
	if d.FixedInterval != "" {
		var err error
		fixedInterval, err = str2duration.ParseDuration(d.FixedInterval)
		if err != nil {
			return err
		}
	}

	var extendedBounds *indexlib.HistogramBound
	if d.ExtendedBounds != nil {
		extendedBounds = &indexlib.HistogramBound{
			Min: utils.UnixToTime(int64(d.ExtendedBounds.Min)).UnixNano(),
			Max: utils.UnixToTime(int64(d.ExtendedBounds.Max)).UnixNano(),
		}
	}
	var hardBounds *indexlib.HistogramBound
	if d.HardBounds != nil {
		hardBounds = &indexlib.HistogramBound{
			Min: utils.UnixToTime(int64(d.HardBounds.Min)).UnixNano(),
			Max: utils.UnixToTime(int64(d.HardBounds.Max)).UnixNano(),
		}
	}

	indexlibAggs.DateHistogram = &indexlib.AggDateHistogram{
		Field: d.Field, CalendarInterval: d.CalendarInterval,
		FixedInterval: int64(fixedInterval), Format: d.Format,
		TimeZone: d.TimeZone, Offset: d.Offset, MinDocCount: d.MinDocCount,
		Keyed: d.Keyed, Missing: d.Missing,
		ExtendedBounds: extendedBounds,
		HardBounds:     hardBounds,
	}
	return nil
}
