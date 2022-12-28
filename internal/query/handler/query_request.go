// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package handler

// TODO: too many query type to be defined

type QueryRequest struct {
	Index string `json:"index"`
	Query Query  `json:"query"`
	Size  int64  `json:"size"`
}

type Query struct {
	// "match_all": {}
	MatchAll *MatchAll `json:"match_all,omitempty"`
	// {"term": {"field": {"value": "value"}}}
	Term map[string]TermVal `json:"term,omitempty"`
}

type MatchAll struct{}

type TermVal struct {
	Value interface{} `json:"value"`
}
