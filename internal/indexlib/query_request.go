// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

type QueryRequest interface {
	searcher()
}

type baseQuery struct {
	Boost float64
}

func (m *baseQuery) searcher() {
}

type MatchAllQuery struct {
	*baseQuery
}

type MatchQuery struct {
	*baseQuery
	Match string
	Field string
}

type TermQuery struct {
	*baseQuery
	Term  string
	Field string
}

type BooleanQuery struct {
	*baseQuery
	Musts    []QueryRequest
	Shoulds  []QueryRequest
	MustNots []QueryRequest
}
