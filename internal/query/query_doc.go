// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package query

import (
	"context"
	"errors"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/manage"
	"github.com/tatris-io/tatris/internal/protocol"
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
	libRequest, err := transform(request)
	if err != nil {
		return nil, err
	}
	limit := libRequest.Query().Size
	resp, err := reader.Search(context.Background(), libRequest, limit)
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

func transform(request protocol.QueryRequest) (indexlib.QueryRequest, error) {
	query := request.Query
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
		}
		return &termQ, nil
	} else if query.Ids != nil {
		ids := *query.Ids
		if len(ids.Values) <= 0 {
			return &indexlib.Terms{}, nil
		}
		termsQ := indexlib.Terms{}
		for _, v := range ids.Values {
			termsQ.Fields = append(termsQ.Fields, v)
		}
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
	} else {
		// TODO: need to be supported
		return nil, errors.New("need to be supported")
	}
}
