// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

import "time"

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
	Terms         *AggTerms
	NumericRange  *AggNumericRange
	DateRange     *AggDateRange
	Count         *AggMetric
	Sum           *AggMetric
	Min           *AggMetric
	Max           *AggMetric
	Avg           *AggMetric
	Cardinality   *AggMetric
	WeightedAvg   *AggWeightedAvg
	Percentiles   *AggPercentiles
	DateHistogram *AggDateHistogram
	Histogram     *AggHistogram
	Filter        *AggFilter
	Aggs          map[string]Aggs
}

type AggPercentiles struct {
	Field       string
	Percents    []float64
	Compression float64
}

type AggMetric struct {
	Field string
}

type AggWeightedAvg struct {
	Value  *AggMetric
	Weight *AggMetric
}

type AggTerms struct {
	Field     string
	Size      int
	ShardSize int
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

type AggDateRange struct {
	Field    string
	Format   string
	TimeZone *time.Location
	Ranges   []DateRange
	Keyed    bool
}

type DateRange struct {
	To   string
	From string
}

type Sort []map[string]SortTerm

type SortTerm struct {
	Order   string
	Missing string
}

type AggDateHistogram struct {
	Field            string
	FixedInterval    int64 // nanos
	CalendarInterval string
	Format           string
	TimeZone         *time.Location
	Offset           any
	MinDocCount      int
	Keyed            bool
	Missing          string
	ExtendedBounds   *DateHistogramBound
	HardBounds       *DateHistogramBound
}

type DateHistogramBound struct {
	Min int64
	Max int64
}

type AggHistogram struct {
	Field          string
	Interval       float64
	MinDocCount    int
	Offset         float64
	Keyed          bool
	Missing        string
	ExtendedBounds *HistogramBound
	HardBounds     *HistogramBound
}

type HistogramBound struct {
	Min float64
	Max float64
}

type AggFilter struct {
	FilterQuery QueryRequest
}
