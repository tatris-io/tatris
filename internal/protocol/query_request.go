// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package protocol describes the core data structures and calling conventions of Tatris
package protocol

type QueryRequest struct {
	Index string `json:"index"`
	Query Query  `json:"query"`
	Size  int64  `json:"size"`
}

// TODO: to be supplemented
type Query struct {
	// "match_all": {}
	MatchAll *MatchAll `json:"match_all,omitempty"`
	// {"match": {"field": "value"}}
	Match *Match `json:"match,omitempty"`
	// {"match_phrase": {"field": "value"}}
	MatchPhrase *MatchPhrase `json:"match_phrase,omitempty"`
	// {"ids": {"values": ["id1", "id2"]}}
	Ids *Ids `json:"ids,omitempty"`
	// {"term": {"field": "value"}}
	Term *Term `json:"term,omitempty"`
	// {"terms": {"field": ["value1", "value2"]}}
	Terms *Terms `json:"terms,omitempty"`
	// {"bool": {"must": [{"term": {"field1": "value1"}}, {"term": {"field2": "value2"}}]}}
	Bool *Bool `json:"bool,omitempty"`
}

type MatchAll struct{}

type Match map[string]interface{}

type MatchPhrase map[string]interface{}

type Ids struct {
	Values []string `json:"values"`
}

type Term map[string]interface{}

type Terms map[string][]interface{}

type Range map[string]*RangeVal

type RangeVal struct {
	Gt  interface{} `json:"gt,omitempty"`
	Gte interface{} `json:"gte,omitempty"`
	Lt  interface{} `json:"lt,omitempty"`
	Lte interface{} `json:"lte,omitempty"`
}

type Bool struct {
	Must               []interface{} `json:"must,omitempty"`
	MustNot            []interface{} `json:"must_not,omitempty"`
	Should             []interface{} `json:"should,omitempty"`
	Filter             interface{}   `json:"filter,omitempty"`
	MinimumShouldMatch string        `json:"minimum_should_match,omitempty"`
}