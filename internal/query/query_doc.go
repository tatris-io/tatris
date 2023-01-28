// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package query organizes codes on the query routine
package query

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/meta/metadata"
	"github.com/tatris-io/tatris/internal/protocol"
	"go.uber.org/zap"
)

func SearchDocs(request protocol.QueryRequest) (*protocol.Hits, error) {
	indexName := request.Index
	index, err := metadata.GetIndex(indexName)
	if err != nil {
		return nil, err
	}
	start, end, err := timeRange(request.Query)
	if err != nil {
		return nil, err
	}
	readers, err := index.GetReadersByTime(start, end)
	if err != nil {
		return nil, err
	}
	hits := &protocol.Hits{
		Total: protocol.Total{Value: 0, Relation: "eq"},
	}
	if len(readers) == 0 {
		return nil, nil
	}
	libRequest, err := transform(request.Query)
	if err != nil {
		return nil, err
	}
	hits.Hits = make([]protocol.Hit, 0)
	var totalValue int64
	totalRelation := "eq"
	for _, reader := range readers {
		resp, err := reader.Search(context.Background(), libRequest, int(request.Size))
		if err != nil {
			return nil, err
		}
		respHits := resp.Hits
		totalValue += respHits.Total.Value
		totalRelation = respHits.Total.Relation
		for _, respHit := range respHits.Hits {
			hits.Hits = append(
				hits.Hits,
				protocol.Hit{Index: respHit.Index, ID: respHit.ID, Source: respHit.Source},
			)
		}
		reader.Close()
	}
	hits.Total.Value = totalValue
	hits.Total.Relation = totalRelation
	return hits, nil
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
		return start, end, fmt.Errorf("invalid time range: %d, %d", start, end)
	}
	return start, end, nil
}

func transform(query protocol.Query) (indexlib.QueryRequest, error) {
	if query.MatchAll != nil {
		return &indexlib.MatchAllQuery{}, nil
	} else if query.Match != nil {
		return transformMatch(query)
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
		return transformBool(query)
	} else {
		// TODO: need to be supported
		return nil, errors.New("need to be supported")
	}
}

func transformMatch(query protocol.Query) (indexlib.QueryRequest, error) {
	matches := query.Match
	if len(matches) <= 0 {
		return nil, errors.New("invalid match query")
	}
	matchQ := indexlib.MatchQuery{}
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
	return &matchQ, nil
}

func transformMatchPhrase(query protocol.Query) (indexlib.QueryRequest, error) {
	matches := query.MatchPhrase
	if len(matches) <= 0 {
		return nil, errors.New("invalid match phrase query")
	}
	matchQ := indexlib.MatchPhraseQuery{}
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
	return &matchQ, nil
}

func transformQueryString(query protocol.Query) (indexlib.QueryRequest, error) {
	querys := query.QueryString
	if len(querys) <= 0 {
		return nil, errors.New("invalid query string query")
	}
	queryStr := indexlib.QueryString{}
	queryStr.Query = querys["query"].(string)
	if analyzer, ok := querys["analyzer"]; ok {
		queryStr.Analyzer = analyzer.(string)
	}

	return &queryStr, nil
}

func transformTerm(query protocol.Query) (indexlib.QueryRequest, error) {
	term := query.Term
	if len(term) <= 0 {
		return &indexlib.TermQuery{}, nil
	}
	termQ := indexlib.TermQuery{}
	for k, v := range term {
		termQ.Field = k
		switch v := v.(type) {
		case string:
			termQ.Term = v
		case map[string]interface{}:
			termQ.Term = v["value"].(string)
		}
	}
	return &termQ, nil
}

func transformIds(query protocol.Query) (indexlib.QueryRequest, error) {
	ids := *query.Ids
	if len(ids.Values) <= 0 {
		return &indexlib.Terms{}, nil
	}
	termsQ := indexlib.Terms{}
	termsQ.Fields = append(termsQ.Fields, ids.Values...)
	return &termsQ, nil
}

func transformTerms(query protocol.Query) (indexlib.QueryRequest, error) {
	terms := query.Terms
	if len(terms) <= 0 {
		return &indexlib.TermsQuery{}, nil
	}
	field := ""
	values := []string{}
	termsQ := indexlib.TermsQuery{}
	for k, v := range terms {
		field = k
		for _, vv := range v {
			switch vv := vv.(type) {
			case string:
				values = append(values, vv)
			default:
				return nil, errors.New("unsupported terms value type")
			}
		}
		termsQ.Terms[field] = &indexlib.Terms{
			Fields: values,
		}
	}
	return &termsQ, nil
}

func transformRange(query protocol.Query) (indexlib.QueryRequest, error) {
	rangeQuery := query.Range
	if len(rangeQuery) <= 0 {
		return &indexlib.RangeQuery{}, nil
	}

	var rangeQ *indexlib.RangeQuery
	for k, v := range rangeQuery {
		rangeQ = &indexlib.RangeQuery{Range: map[string]*indexlib.RangeVal{
			k: {
				GT:  v.Gt,
				GTE: v.Gte,
				LT:  v.Lt,
				LTE: v.Lte,
			},
		}}
	}
	return rangeQ, nil
}

func transformBool(query protocol.Query) (indexlib.QueryRequest, error) {
	q := &indexlib.BooleanQuery{}

	if query.Bool.Must != nil {
		q.Musts = make([]indexlib.QueryRequest, 0, len(query.Bool.Must))
		for _, must := range query.Bool.Must {
			queryRequest, err := transform(*must)
			if err != nil {
				return nil, errors.New("bool query must transform error")
			}
			q.Musts = append(q.Musts, queryRequest)
		}
	}
	if query.Bool.MustNot != nil {
		q.MustNots = make([]indexlib.QueryRequest, 0, len(query.Bool.MustNot))
		for _, mustNot := range query.Bool.MustNot {
			queryRequest, err := transform(*mustNot)
			if err != nil {
				return nil, errors.New("bool query mustNot transform error")
			}
			q.MustNots = append(q.MustNots, queryRequest)
		}
	}
	if query.Bool.Should != nil {
		q.Shoulds = make([]indexlib.QueryRequest, 0, len(query.Bool.Should))
		for _, should := range query.Bool.Should {
			queryRequest, err := transform(*should)
			if err != nil {
				return nil, errors.New("bool query should transform error")
			}
			q.Shoulds = append(q.Shoulds, queryRequest)
		}
	}
	if query.Bool.Filter != nil {
		q.Filters = make([]indexlib.QueryRequest, 0, len(query.Bool.Filter))
		for _, filter := range query.Bool.Filter {
			queryRequest, err := transform(*filter)
			if err != nil {
				return nil, errors.New("bool query filter transform error")
			}
			q.Filters = append(q.Filters, queryRequest)
		}
	}
	if query.Bool.MinimumShouldMatch != "" {
		minShould, err := strconv.Atoi(query.Bool.MinimumShouldMatch)
		if err != nil {
			return nil, errors.New("bool query minimumShouldMatch transform error")
		}
		q.MinShould = minShould
	}
	return q, nil
}
