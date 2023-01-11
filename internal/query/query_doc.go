// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package query

import (
	"context"
	"errors"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/manage"
	"github.com/tatris-io/tatris/internal/protocol"
	"strconv"
)

// TODO: make it configurable
var dataPath = "/tmp/tatris/_data"

func SearchDocs(request protocol.QueryRequest) (*protocol.Hits, error) {
	config := &indexlib.BaseConfig{
		Index:    request.Index,
		DataPath: dataPath,
	}
	reader, err := manage.GetReader(config)
	if err != nil {
		return nil, err
	}
	libRequest, err := transform(request.Query)
	if err != nil {
		return nil, err
	}
	resp, err := reader.Search(context.Background(), libRequest, int(request.Size))
	if err != nil {
		return nil, err
	}
	respHits := resp.Hits
	hits := &protocol.Hits{
		Total: protocol.Total{Value: respHits.Total.Value, Relation: respHits.Total.Relation},
	}
	hits.Hits = make([]protocol.Hit, len(respHits.Hits))
	for i, respHit := range respHits.Hits {
		hits.Hits[i] = protocol.Hit{Index: respHit.Index, ID: respHit.ID, Source: respHit.Source}
	}
	return hits, nil
}

func transform(query protocol.Query) (indexlib.QueryRequest, error) {
	if query.MatchAll != nil {
		return &indexlib.MatchAllQuery{}, nil
	} else if query.Match != nil {
		matches := *query.Match
		if len(matches) != 1 {
			return nil, errors.New("invalid match query")
		}
		matchQ := indexlib.MatchQuery{}
		for k, v := range matches {
			matchQ.Field = k
			matchQ.Match = v.(string)
		}
		return &matchQ, nil
	} else if query.Term != nil {
		term := *query.Term
		if len(term) <= 0 {
			return &indexlib.TermQuery{}, nil
		}
		termQ := indexlib.TermQuery{}
		for k, v := range term {
			termQ.Field = k
			termQ.Term = v.(string)
			//termQ.Term = v.(map[string]interface{})["value"].(string)
		}
		return &termQ, nil
	} else if query.Ids != nil {
		ids := *query.Ids
		if len(ids.Values) <= 0 {
			return &indexlib.Terms{}, nil
		}
		termsQ := indexlib.Terms{}
		termsQ.Fields = append(termsQ.Fields, ids.Values...)
		return &termsQ, nil
	} else if query.Terms != nil {
		terms := *query.Terms
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
	} else if query.Range != nil {
		rangeQuery := *query.Range
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
	} else if query.Bool != nil {
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
	} else {
		// TODO: need to be supported
		return nil, errors.New("need to be supported")
	}
}
