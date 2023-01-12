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
	Match     string
	Field     string
	Analyzer  string // STANDARD(default), KEYWORD, SIMPLE, WEB
	Prefix    int    // Defaults to 0
	Fuzziness int
	Operator  string // OR, AND
}

type MatchPhraseQuery struct {
	*BaseQuery
	MatchPhrase string
	Field       string
	Analyzer    string
	Slop        int // Defaults to 0
}

type QueryString struct {
	*BaseQuery
	Query    string
	Analyzer string
}

type TermQuery struct {
	*BaseQuery
	Term  string
	Field string
}

type TermsQuery struct {
	*BaseQuery
	Terms map[string]*Terms
}

type Terms struct {
	*BaseQuery
	Fields []string
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
	Range map[string]*RangeVal
}

type RangeVal struct {
	GT  interface{}
	GTE interface{}
	LT  interface{}
	LTE interface{}
}
