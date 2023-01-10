// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

type Query struct {
	Size int `json:"size"`
}

type QueryRequest interface {
	searcher()
	Query() Query
}

type baseQuery struct {
	Boost float64
}

func (m *baseQuery) searcher() {
}

func (m *baseQuery) Query() Query {
	query := Query{
		Size: -1,
	}
	return query
}

type MatchAllQuery struct {
	*baseQuery
}

type MatchQuery struct {
	*baseQuery
	Match string
	Field string
}

func (m *MatchQuery) searcher() {
}

type TermQuery struct {
	*baseQuery
	Term  string
	Field string
}

func (m *TermQuery) Query() Query {
	query := Query{
		Size: 10,
	}
	return query
}

type TermsQuery struct {
	*baseQuery
	Terms map[string]*Terms `json:"terms,omitempty"`
}

func (m *TermsQuery) Query() Query {
	query := Query{
		Size: 10,
	}
	return query
}

type Terms struct {
	*baseQuery
	Fields []string `json:"fields"`
}

func (m *Terms) Query() Query {
	query := Query{
		Size: 10,
	}
	return query
}

type BooleanQuery struct {
	*baseQuery
	Musts    []QueryRequest
	Shoulds  []QueryRequest
	MustNots []QueryRequest
}

type RangeQuery struct {
	Range map[string]*RangeVal `json:"range,omitempty"`
}

type RangeVal struct {
	GT  interface{} `json:"gt,omitempty"`  // null, float64
	GTE interface{} `json:"gte,omitempty"` // null, float64
	LT  interface{} `json:"lt,omitempty"`  // null, float64
	LTE interface{} `json:"lte,omitempty"` // null, float64
}

func (m *RangeQuery) searcher() {
}

func (m *RangeQuery) Query() Query {
	query := Query{
		Size: 10,
	}
	return query
}
