// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

type QueryRequest interface {
	SetAggs(aggregations map[string]Aggs)
	GetAggs() map[string]Aggs
	SetSort(sort Sort)
	GetSort() Sort
}

type BaseQuery struct {
	Boost float64
	Aggs  map[string]Aggs
	Sort  Sort
}

func NewBaseQuery() *BaseQuery {
	return &BaseQuery{}
}

func (m *BaseQuery) SetAggs(aggs map[string]Aggs) {
	m.Aggs = aggs
}

func (m *BaseQuery) GetAggs() map[string]Aggs {
	return m.Aggs
}

func (m *BaseQuery) SetSort(sort Sort) {
	m.Sort = sort
}

func (m *BaseQuery) GetSort() Sort {
	return m.Sort
}

type MatchAllQuery struct {
	*BaseQuery
}

func NewMatchAllQuery() *MatchAllQuery {
	return &MatchAllQuery{BaseQuery: NewBaseQuery()}
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

func NewMatchQuery() *MatchQuery {
	return &MatchQuery{BaseQuery: NewBaseQuery()}
}

type MatchPhraseQuery struct {
	*BaseQuery
	MatchPhrase string
	Field       string
	Analyzer    string
	Slop        int // Defaults to 0
}

func NewMatchPhraseQuery() *MatchPhraseQuery {
	return &MatchPhraseQuery{BaseQuery: NewBaseQuery()}
}

type QueryString struct {
	*BaseQuery
	Query    string
	Analyzer string
}

func NewQueryString() *QueryString {
	return &QueryString{BaseQuery: NewBaseQuery()}
}

type TermQuery struct {
	*BaseQuery
	Term  string
	Field string
}

func NewTermQuery() *TermQuery {
	return &TermQuery{BaseQuery: NewBaseQuery()}
}

type TermsQuery struct {
	*BaseQuery
	Terms map[string]*Terms
}

func NewTermsQuery() *TermsQuery {
	return &TermsQuery{BaseQuery: NewBaseQuery()}
}

type Terms struct {
	*BaseQuery
	Fields []string
}

func NewTerms() *Terms {
	return &Terms{BaseQuery: NewBaseQuery()}
}

type BooleanQuery struct {
	*BaseQuery
	Musts     []QueryRequest
	Shoulds   []QueryRequest
	MustNots  []QueryRequest
	Filters   []QueryRequest
	MinShould int
}

func NewBooleanQuery() *BooleanQuery {
	return &BooleanQuery{BaseQuery: NewBaseQuery()}
}

type RangeQuery struct {
	*BaseQuery
	Range map[string]*RangeVal
}

func NewRangeQuery() *RangeQuery {
	return &RangeQuery{BaseQuery: NewBaseQuery()}
}

type RangeVal struct {
	GT  interface{}
	GTE interface{}
	LT  interface{}
	LTE interface{}
}

type Aggs struct {
	Terms        *AggTerms
	NumericRange *AggNumericRange
	Sum          *AggMetric
	Min          *AggMetric
	Max          *AggMetric
	Avg          *AggMetric
	Cardinality  *AggMetric
	WeightedAvg  *AggWeightedAvg
	Aggs         map[string]Aggs
}

type AggMetric struct {
	Field string
}

type AggWeightedAvg struct {
	Value  *AggMetric
	Weight *AggMetric
}

type AggTerms struct {
	Field string
	Size  int
}

type AggNumericRange struct {
	Field  string
	Ranges []NumericRange
	Keyed  bool
}

type NumericRange struct {
	To   float64
	From float64
}

type Sort []map[string]SortTerm

type SortTerm struct {
	Order string
}
