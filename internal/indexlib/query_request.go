// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

type QueryRequest interface {
	searcher()
}

type MatchAllQuery struct {
}

func (m *MatchAllQuery) searcher() {
}

type MatchQuery struct {
	Match string
	Field string
}

func (m *MatchQuery) searcher() {
}

type TermQuery struct {
	Term  string
	Field string
}

func (m *TermQuery) searcher() {
}

type BooleanQuery struct {
	Musts    *[]QueryRequest
	Shoulds  *[]QueryRequest
	MustNots *[]QueryRequest
}

func (m *BooleanQuery) searcher() {
}
