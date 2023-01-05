// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package query

import (
	"context"
	"errors"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/manage"
	"github.com/tatris-io/tatris/internal/protocol"
	"os"
)

var wd, _ = os.Getwd()

// TODO: make it configurable
var dataPath = wd + "/../../../_data"

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
	resp, err := reader.Search(context.Background(), libRequest, -1)
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
	} else {
		// TODO: need to be supported
		return nil, errors.New("need to be supported")
	}
}
