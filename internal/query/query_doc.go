// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package query organizes codes on the query routine
package query

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/tatris-io/tatris/internal/common/log/logger"
	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/core/config"

	"github.com/tatris-io/tatris/internal/common/utils"
	str2duration "github.com/xhit/go-str2duration/v2"

	"github.com/tatris-io/tatris/internal/core"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/protocol"
)

func SearchDocs(
	indexes []*core.Index,
	request protocol.QueryRequest,
) (*protocol.QueryResponse, error) {
	if request.Size < 0 {
		request.Size = 10
	}
	if request.From < 0 {
		request.From = 0
	}
	start, end, err := resolveTimeRange(request.Index, request.Query)
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
		DataPath: config.Cfg.GetDataPath(),
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

	var aggsNamePrefixDict map[string]string
	if request.TypedKeys {
		aggsNamePrefixDict = make(map[string]string)
	}

	var aggs map[string]protocol.Aggs
	if request.Aggs != nil {
		aggs = request.Aggs
	} else {
		aggs = request.Aggregations
	}
	if aggs != nil {
		agg, err := transformAggs(aggs, indexes[0].Mappings, aggsNamePrefixDict)
		if err != nil {
			return nil, err
		}
		libRequest.SetAggs(agg)
	}

	if sort := request.Sort; sort != nil {
		libRequest.SetSort(transformSort(sort))
	}

	hits.Hits = make([]protocol.Hit, 0)
	aggregations := make(map[string]protocol.Aggregation)

	resp, err := reader.Search(
		context.Background(),
		libRequest,
		int(request.Size),
		int(request.From),
	)
	if err != nil {
		return nil, err
	}
	respHits := resp.Hits
	for _, respHit := range respHits.Hits {
		hits.Hits = append(
			hits.Hits,
			protocol.Hit{
				Index:  respHit.Index,
				ID:     respHit.ID,
				Source: respHit.Source,
				Score:  respHit.Score,
				Type:   respHit.Type,
			},
		)
	}

	for k, v := range resp.Aggregations {
		if aggsNamePrefixDict != nil {
			if prefix, ok := aggsNamePrefixDict[k]; ok {
				k = strings.Join([]string{prefix, k}, consts.TypedKeysDelimiter)
			}
		}
		aggregations[k] = protocol.Aggregation{
			Value:   v.Value,
			Buckets: addAggNamePrefix(v.Buckets, aggsNamePrefixDict),
		}
	}

	hits.Total.Value = respHits.Total.Value
	hits.Total.Relation = respHits.Total.Relation
	hits.MaxScore = respHits.MaxScore
	return &protocol.QueryResponse{Hits: hits, Aggregations: aggregations}, nil
}

func resolveTimeRange(index string, query protocol.Query) (int64, int64, error) {
	start, end, err := timeRange(query)
	if err != nil {
		return 0, 0, err
	}
	if start > 0 && end > 0 && start <= end {
		return start, end, nil
	}
	// default scan range: the number of past hours specified by
	// config.Cfg.Query.DefaultScanHours closest to the target time
	now := time.Now().UnixMilli()
	if end == 0 || end > now {
		end = now
	}
	if start == 0 || start > end {
		start = end - (time.Hour.Milliseconds() * int64(config.Cfg.Query.DefaultScanHours))
	}
	logger.Info(
		"unable to resolve time range from request, set to default",
		zap.String("index", index),
		zap.Int64("start", start),
		zap.Int64("end", end),
	)
	return start, end, nil
}

func timeRange(query protocol.Query) (int64, int64, error) {
	var start, end int64
	var err error
	if query.Range != nil {
		timeRange, ok := query.Range[consts.TimestampField]
		if ok {
			if timeRange.Gt != nil {
				t, err := utils.ParseTime(timeRange.Gt)
				if err != nil {
					return 0, 0, err
				}
				start = t.UnixMilli() + 1
			}
			if timeRange.Gte != nil {
				t, err := utils.ParseTime(timeRange.Gte)
				if err != nil {
					return 0, 0, err
				}
				start = t.UnixMilli()
			}
			if timeRange.Lt != nil {
				t, err := utils.ParseTime(timeRange.Lt)
				if err != nil {
					return 0, 0, err
				}
				end = t.UnixMilli() - 1
			}
			if timeRange.Lte != nil {
				t, err := utils.ParseTime(timeRange.Lte)
				if err != nil {
					return 0, 0, err
				}
				end = t.UnixMilli()
			}
		}
	} else if query.Bool != nil {
		subQueries := make([]*protocol.Query, 0)
		subQueries = append(subQueries, query.Bool.Must...)
		subQueries = append(subQueries, query.Bool.Should...)
		subQueries = append(subQueries, query.Bool.Filter...)
		for _, subQuery := range subQueries {
			start, end, err = timeRange(*subQuery)
			if err != nil || start > 0 || end > 0 {
				return start, end, err
			}
		}
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
		return transformRange(query, mappings)
	} else if query.Bool != nil {
		return transformBool(query, mappings)
	} else {
		// Exposed queries allow users to specify no query, example: "{"size": xx}"
		// Use match all query when query is nil
		return transformMatchAll()
	}
}

func transformAggs(
	aggs map[string]protocol.Aggs,
	mappings *protocol.Mappings,
	aggsPrefixDict map[string]string,
) (map[string]indexlib.Aggs, error) {
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
				agg.Terms.ShardSize = config.Cfg.Query.DefaultAggregationShardSize
			}
			if aggsPrefixDict != nil {
				aggsPrefixDict[name] = consts.TypedKeysStermsPrefix
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
			if agg.NumericRange.Ranges == nil || len(agg.NumericRange.Ranges) == 0 {
				return nil, fmt.Errorf("no [ranges] specified for the range aggregation [%s]", name)
			}
			err := validateAggFieldType(mappings, agg.NumericRange.Field, consts.LibFieldTypeNumeric, name, "range")
			if err != nil {
				return nil, err
			}
			if aggsPrefixDict != nil {
				aggsPrefixDict[name] = consts.TypedKeysRangePrefix
			}
			indexLibRanges := make([]indexlib.NumericRange, 0, len(agg.NumericRange.Ranges))
			if ranges := agg.NumericRange.Ranges; ranges != nil {
				for _, r := range ranges {
					indexLibRanges = append(indexLibRanges, indexlib.NumericRange{From: r.From, To: r.To})
				}
			}
			indexlibAggs.NumericRange = &indexlib.AggNumericRange{Field: agg.NumericRange.Field, Ranges: indexLibRanges, Keyed: agg.NumericRange.Keyed}
		} else if agg.DateRange != nil {
			if agg.DateRange.Field == "" {
				return nil, errs.ErrEmptyField
			}
			if agg.DateRange.Ranges == nil || len(agg.DateRange.Ranges) == 0 {
				return nil, fmt.Errorf("no [ranges] specified for the date_range aggregation [%s]", name)
			}
			err := validateAggFieldType(mappings, agg.DateRange.Field, consts.LibFieldTypeDate, name, "date_range")
			if err != nil {
				return nil, err
			}
			if aggsPrefixDict != nil {
				aggsPrefixDict[name] = consts.TypedKeysDateRangePrefix
			}
			indexLibRanges := make([]indexlib.DateRange, 0, len(agg.DateRange.Ranges))
			if ranges := agg.DateRange.Ranges; ranges != nil {
				for _, r := range ranges {
					indexLibRanges = append(indexLibRanges, indexlib.DateRange{From: r.From, To: r.To})
				}
			}
			indexlibAggs.DateRange = &indexlib.AggDateRange{
				Field:    agg.DateRange.Field,
				Ranges:   indexLibRanges,
				Format:   agg.DateRange.Format,
				TimeZone: utils.ParseTimeZone(agg.DateRange.TimeZone),
				Keyed:    agg.DateRange.Keyed,
			}
		} else if agg.Filter != nil {
			filterQuery, err := transform(*agg.Filter, mappings)
			if err != nil {
				return nil, err
			}
			if aggsPrefixDict != nil {
				aggsPrefixDict[name] = consts.TypedKeysFilterPrefix
			}
			indexlibAggs.Filter = &indexlib.AggFilter{FilterQuery: filterQuery}
		} else if agg.Count != nil {
			if aggsPrefixDict != nil {
				aggsPrefixDict[name] = consts.TypedKeysCountPrefix
			}
			indexlibAggs.Count = &indexlib.AggMetric{Field: agg.Count.Field}
		} else if agg.ValueCount != nil {
			if aggsPrefixDict != nil {
				aggsPrefixDict[name] = consts.TypedKeysCountPrefix
			}
			indexlibAggs.Count = &indexlib.AggMetric{Field: agg.ValueCount.Field}
		} else if agg.Sum != nil {
			if agg.Sum.Field == "" {
				return nil, errs.ErrEmptyField
			}
			err := validateAggFieldType(mappings, agg.Sum.Field, consts.LibFieldTypeNumeric, name, "sum")
			if err != nil {
				return nil, err
			}
			if aggsPrefixDict != nil {
				aggsPrefixDict[name] = consts.TypedKeysSumPrefix
			}
			indexlibAggs.Sum = &indexlib.AggMetric{Field: agg.Sum.Field}
		} else if agg.Min != nil {
			if agg.Min.Field == "" {
				return nil, errs.ErrEmptyField
			}
			err := validateAggFieldType(mappings, agg.Min.Field, consts.LibFieldTypeNumeric, name, "min")
			if err != nil {
				return nil, err
			}
			if aggsPrefixDict != nil {
				aggsPrefixDict[name] = consts.TypedKeysMinPrefix
			}
			indexlibAggs.Min = &indexlib.AggMetric{Field: agg.Min.Field}
		} else if agg.Max != nil {
			if agg.Max.Field == "" {
				return nil, errs.ErrEmptyField
			}
			err := validateAggFieldType(mappings, agg.Max.Field, consts.LibFieldTypeNumeric, name, "max")
			if err != nil {
				return nil, err
			}
			if aggsPrefixDict != nil {
				aggsPrefixDict[name] = consts.TypedKeysMaxPrefix
			}
			indexlibAggs.Max = &indexlib.AggMetric{Field: agg.Max.Field}
		} else if agg.Avg != nil {
			if agg.Avg.Field == "" {
				return nil, errs.ErrEmptyField
			}
			err := validateAggFieldType(mappings, agg.Avg.Field, consts.LibFieldTypeNumeric, name, "avg")
			if err != nil {
				return nil, err
			}
			if aggsPrefixDict != nil {
				aggsPrefixDict[name] = consts.TypedKeysAvgPrefix
			}
			indexlibAggs.Avg = &indexlib.AggMetric{Field: agg.Avg.Field}
		} else if agg.WeightedAvg != nil {
			if agg.WeightedAvg.Value.Field == "" {
				return nil, errs.ErrEmptyField
			}
			err := validateAggFieldType(mappings, agg.WeightedAvg.Value.Field, consts.LibFieldTypeNumeric, name, "weighted_avg")
			if err != nil {
				return nil, err
			}
			if aggsPrefixDict != nil {
				aggsPrefixDict[name] = consts.TypedKeysWeightedAvgPrefix
			}
			indexlibAggs.WeightedAvg = &indexlib.AggWeightedAvg{
				Value:  &indexlib.AggMetric{Field: agg.WeightedAvg.Value.Field},
				Weight: &indexlib.AggMetric{Field: agg.WeightedAvg.Weight.Field},
			}
		} else if agg.Cardinality != nil {
			if agg.Cardinality.Field == "" {
				return nil, errs.ErrEmptyField
			}
			if aggsPrefixDict != nil {
				aggsPrefixDict[name] = consts.TypedKeysCardinalityPrefix
			}
			indexlibAggs.Cardinality = &indexlib.AggMetric{Field: agg.Cardinality.Field}
		} else if agg.Percentiles != nil {
			err := transformPercentilesAgg(mappings, name, agg.Percentiles, indexlibAggs, aggsPrefixDict)
			if err != nil {
				return nil, err
			}
		} else if agg.DateHistogram != nil {
			err := transformDateHistogramAgg(mappings, name, agg.DateHistogram, indexlibAggs, aggsPrefixDict)
			if err != nil {
				return nil, err
			}
		} else if agg.Histogram != nil {
			err := transformHistogramAgg(mappings, name, agg.Histogram, indexlibAggs, aggsPrefixDict)
			if err != nil {
				return nil, err
			}
		}

		// sub-aggregations
		var subAggs map[string]protocol.Aggs
		if agg.Aggs != nil {
			subAggs = agg.Aggs
		} else {
			subAggs = agg.Aggregations
		}

		if subAggs != nil {
			var err error
			indexlibAggs.Aggs, err = transformAggs(subAggs, mappings, aggsPrefixDict)
			if err != nil {
				return nil, err
			}
		}

		result[name] = *indexlibAggs
	}

	return result, nil
}

func validateAggFieldType(
	mappings *protocol.Mappings,
	field string,
	needFieldType string,
	aggName string,
	aggType string,
) error {
	if fieldType, ok := mappings.Properties[field]; ok {
		ok, lType := indexlib.ValidateMappingType(fieldType.Type)
		if ok && lType.Type != needFieldType {
			return &errs.InvalidAggFieldTypeError{
				Field:           field,
				FieldType:       lType.Type,
				AggregationType: aggType,
				AggregationName: aggName,
			}
		}
	}
	return nil
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
	termsQ.Terms = make(map[string]*indexlib.Terms, len(terms))
	for k, v := range terms {
		if strings.ToLower(k) == "boost" {
			// TODO
			continue
		}
		field = k
		switch v := v.(type) {
		case []interface{}:
			for _, vv := range v {
				switch vv := vv.(type) {
				case string:
					values = append(values, vv)
				default:
					return nil, &errs.UnsupportedError{Desc: "term", Value: vv}
				}
			}
		}

		termsQ.Terms[field] = &indexlib.Terms{
			Fields: values,
		}
	}
	return termsQ, nil
}

func transformRange(
	query protocol.Query,
	mappings *protocol.Mappings,
) (indexlib.QueryRequest, error) {

	rangeQuery := query.Range
	if len(rangeQuery) <= 0 {
		return &indexlib.RangeQuery{}, nil
	}
	rangeQ := indexlib.NewRangeQuery()
	for k, v := range rangeQuery {
		property, found := mappings.Properties[k]
		if !found {
			return nil, &errs.InvalidFieldError{Field: k, Message: "not found"}
		}
		_, lType := indexlib.ValidateMappingType(property.Type)
		var gt, gte, lt, lte any
		var err error

		// adapts the elasticsearch query sdk
		if v.From != nil {
			if v.IncludeLower {
				gte = v.From
			} else {
				gt = v.From
			}
		}
		if v.To != nil {
			if v.IncludeUpper {
				lte = v.To
			} else {
				lt = v.To
			}
		}
		switch lType.Type {
		case consts.LibFieldTypeNumeric, consts.LibFieldTypeBool:
			if v.Gt != nil {
				if gt, err = utils.ToFloat64(v.Gt); err != nil {
					return nil, err
				}
			}
			if v.Gte != nil {
				if gte, err = utils.ToFloat64(v.Gte); err != nil {
					return nil, err
				}
			}
			if v.Lt != nil {
				if lt, err = utils.ToFloat64(v.Lt); err != nil {
					return nil, err
				}
			}
			if v.Lte != nil {
				if lte, err = utils.ToFloat64(v.Lte); err != nil {
					return nil, err
				}
			}
		case consts.LibFieldTypeKeyword, consts.LibFieldTypeText:
			if v.Gt != nil {
				gt = utils.ToString(v.Gt)
			}
			if v.Gte != nil {
				gte = utils.ToString(v.Gte)
			}
			if v.Lt != nil {
				lt = utils.ToString(v.Lt)
			}
			if v.Lte != nil {
				lte = utils.ToString(v.Lte)
			}
		case consts.LibFieldTypeDate:
			if v.Gt != nil {
				if gt, err = utils.ParseTime(v.Gt); err != nil {
					return nil, err
				}
			}
			if v.Gte != nil {
				if gte, err = utils.ParseTime(v.Gte); err != nil {
					return nil, err
				}
			}
			if v.Lt != nil {
				if lt, err = utils.ParseTime(v.Lt); err != nil {
					return nil, err
				}
			}
			if v.Lte != nil {
				if lte, err = utils.ParseTime(v.Lte); err != nil {
					return nil, err
				}
			}
		default:
			return nil, &errs.UnsupportedError{Desc: "unsortable type", Value: property.Type}
		}
		rangeQ.Range = map[string]*indexlib.RangeVal{
			k: {
				LT:  lt,
				LTE: lte,
				GT:  gt,
				GTE: gte,
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

func transformDateHistogramAgg(
	mappings *protocol.Mappings,
	aggName string,
	d *protocol.AggDateHistogram,
	indexlibAggs *indexlib.Aggs,
	aggsPrefixDict map[string]string,
) error {
	if d.Field == "" {
		return errs.ErrEmptyField
	}
	err := validateAggFieldType(
		mappings,
		d.Field,
		consts.LibFieldTypeDate,
		aggName,
		"date_histogram",
	)
	if err != nil {
		return err
	}
	if d.Interval != "" && d.FixedInterval == "" {
		d.FixedInterval = d.Interval
	}
	if d.FixedInterval == "" && d.CalendarInterval == "" {
		return fmt.Errorf(
			"required one of fields [fixed_interval, calendar_interval] for date_histogram aggregation [%s] , but none were specified",
			aggName,
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
	if aggsPrefixDict != nil {
		aggsPrefixDict[aggName] = consts.TypedKeysDateHistogramPrefix
	}
	var extendedBounds *indexlib.DateHistogramBound
	if d.ExtendedBounds != nil {
		extendedBounds = &indexlib.DateHistogramBound{
			Min: utils.UnixToTime(int64(d.ExtendedBounds.Min)).UnixNano(),
			Max: utils.UnixToTime(int64(d.ExtendedBounds.Max)).UnixNano(),
		}
	}
	var hardBounds *indexlib.DateHistogramBound
	if d.HardBounds != nil {
		hardBounds = &indexlib.DateHistogramBound{
			Min: utils.UnixToTime(int64(d.HardBounds.Min)).UnixNano(),
			Max: utils.UnixToTime(int64(d.HardBounds.Max)).UnixNano(),
		}
	}

	indexlibAggs.DateHistogram = &indexlib.AggDateHistogram{
		Field: d.Field, CalendarInterval: d.CalendarInterval,
		FixedInterval: int64(fixedInterval), Format: d.Format,
		TimeZone: utils.ParseTimeZone(d.TimeZone), Offset: d.Offset, MinDocCount: d.MinDocCount,
		Keyed: d.Keyed, Missing: d.Missing,
		ExtendedBounds: extendedBounds,
		HardBounds:     hardBounds,
	}
	return nil
}

func transformHistogramAgg(
	mappings *protocol.Mappings,
	aggName string,
	d *protocol.AggHistogram,
	indexlibAggs *indexlib.Aggs,
	aggsPrefixDict map[string]string,
) error {
	if d.Field == "" {
		return errs.ErrEmptyField
	}
	err := validateAggFieldType(mappings, d.Field, consts.LibFieldTypeNumeric, aggName, "histogram")
	if err != nil {
		return err
	}
	if d.Interval <= 0 {
		return fmt.Errorf("[interval] must be >0 for histogram aggregation [%s]", aggName)
	}
	if d.Offset >= d.Interval {
		return fmt.Errorf(
			"[offset] must be in [0, interval) for histogram aggregation [%s]",
			aggName,
		)
	}
	if aggsPrefixDict != nil {
		aggsPrefixDict[aggName] = consts.TypedKeysHistogramPrefix
	}
	var extendedBounds *indexlib.HistogramBound
	if d.ExtendedBounds != nil {
		extendedBounds = &indexlib.HistogramBound{
			Min: d.ExtendedBounds.Min,
			Max: d.ExtendedBounds.Max,
		}
	}
	var hardBounds *indexlib.HistogramBound
	if d.HardBounds != nil {
		hardBounds = &indexlib.HistogramBound{
			Min: d.HardBounds.Min,
			Max: d.HardBounds.Max,
		}
	}

	indexlibAggs.Histogram = &indexlib.AggHistogram{
		Field:          d.Field,
		Interval:       d.Interval,
		Offset:         d.Offset,
		MinDocCount:    d.MinDocCount,
		Keyed:          d.Keyed,
		Missing:        d.Missing,
		ExtendedBounds: extendedBounds,
		HardBounds:     hardBounds,
	}
	return nil
}

func transformPercentilesAgg(
	mappings *protocol.Mappings,
	aggName string,
	d *protocol.AggPercentiles,
	indexlibAggs *indexlib.Aggs,
	aggsPrefixDict map[string]string,
) error {
	if d.Field == "" {
		return errs.ErrEmptyField
	}
	err := validateAggFieldType(
		mappings,
		d.Field,
		consts.LibFieldTypeNumeric,
		aggName,
		"percentiles",
	)
	if err != nil {
		return err
	}
	if d.Compression < 1 {
		d.Compression = 100
	}
	if d.Percents == nil || len(d.Percents) == 0 {
		return fmt.Errorf("[percents] must not be empty for percentiles aggregation [%s]", aggName)
	}
	for _, percent := range d.Percents {
		if percent < 0 || percent > 100 {
			return fmt.Errorf(
				"[percents] must be between 0 and 100  for percentiles aggregation [%s]",
				aggName,
			)
		}
	}
	if aggsPrefixDict != nil {
		aggsPrefixDict[aggName] = consts.TypedKeysPercentilesPrefix
	}
	indexlibAggs.Percentiles = &indexlib.AggPercentiles{
		Field:       d.Field,
		Percents:    d.Percents,
		Compression: d.Compression,
	}
	return nil
}

// addAggNamePrefix will be prefixing aggregation names with their type
func addAggNamePrefix(
	buckets []protocol.Bucket,
	aggsNamePrefixDict map[string]string,
) []protocol.Bucket {
	for _, bucket := range buckets {
		for name, prefix := range aggsNamePrefixDict {
			if v, ok := bucket[name]; ok {
				newName := strings.Join([]string{prefix, name}, consts.TypedKeysDelimiter)
				bucket[newName] = v
				if v, ok := v.(indexlib.Aggregation); ok {
					addAggNamePrefix(v.Buckets, aggsNamePrefixDict)
					// filter response need remove buckets
					if prefix == consts.TypedKeysFilterPrefix && v.Buckets != nil && len(v.Buckets) > 0 {
						bucket[newName] = v.Buckets[0]
					}
					// percentiles response value -> values
					if prefix == consts.TypedKeysPercentilesPrefix && v.Value != nil {
						bucket[newName] = map[string]interface{}{"values": v.Value}
					}
				}

				delete(bucket, name)
			}
		}
	}
	return buckets
}
