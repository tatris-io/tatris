// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

type QueryRequest interface {
	searcher()
}

type BaseQuery struct {
	Boost float64
}

func (m *BaseQuery) searcher() {
}

type MatchAllQuery struct {
	*BaseQuery
}

type MatchQuery struct {
	*BaseQuery
	Match string
	Field string
}

type TermQuery struct {
	*BaseQuery
	Term  string
	Field string
}

type TermsQuery struct {
	*BaseQuery
	Terms map[string]*Terms `json:"terms,omitempty"`
}

type Terms struct {
	*BaseQuery
	Fields []string `json:"fields"`
}

type BooleanQuery struct {
	*BaseQuery
	Musts     []QueryRequest
	Shoulds   []QueryRequest
	MustNots  []QueryRequest
	Filters   []QueryRequest
	MinShould int
}

type RangeQuery struct {
	*BaseQuery
	Range map[string]*RangeVal `json:"range,omitempty"`
}

type RangeVal struct {
	GT  interface{} `json:"gt,omitempty"`  // null, float64
	GTE interface{} `json:"gte,omitempty"` // null, float64
	LT  interface{} `json:"lt,omitempty"`  // null, float64
	LTE interface{} `json:"lte,omitempty"` // null, float64
}
