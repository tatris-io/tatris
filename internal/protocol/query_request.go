// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package protocol describes the core data structures and calling conventions of Tatris
package protocol

type QueryRequest struct {
	Index string          `json:"index"`
	Query Query           `json:"query"`
	Aggs  map[string]Aggs `json:"aggs"`
	Sort  Sort            `json:"sort"`
	Size  int64           `json:"size"`
}

// TODO: to be supplemented
type Query struct {
	// "match_all": {}
	MatchAll *MatchAll `json:"match_all,omitempty"`
	// {"match": {"field": "value"}}
	Match Match `json:"match,omitempty"`
	// {"match_phrase": {"field": "value"}}
	MatchPhrase MatchPhrase `json:"match_phrase,omitempty"`
	// {"query_string": {"query": "field:value"}}
	QueryString QueryString `json:"query_string,omitempty"`
	// {"ids": {"values": ["id1", "id2"]}}
	Ids *Ids `json:"ids,omitempty"`
	// {"term": {"field": "value"}}
	Term Term `json:"term,omitempty"`
	// {"terms": {"field": ["value1", "value2"]}}
	Terms Terms `json:"terms,omitempty"`
	// {"bool": {"must": [{"term": {"field1": "value1"}}, {"term": {"field2": "value2"}}]}}
	Bool *Bool `json:"bool,omitempty"`
	// {"range": {"field": {"gt": 10, "lt": 20}}}
	Range Range `json:"range,omitempty"`
}

type MatchAll struct{}

type Match map[string]interface{}

type MatchPhrase map[string]interface{}

type QueryString map[string]interface{}

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
	Must               []*Query `json:"must,omitempty"`
	MustNot            []*Query `json:"must_not,omitempty"`
	Should             []*Query `json:"should,omitempty"`
	Filter             []*Query `json:"filter,omitempty"`
	MinimumShouldMatch string   `json:"minimum_should_match,omitempty"`
}

type Aggs struct {
	Terms        *AggTerms        `json:"terms,omitempty"`
	NumericRange *AggNumericRange `json:"range"`
	Sum          *AggMetric       `json:"sum,omitempty"`
	Min          *AggMetric       `json:"min,omitempty"`
	Max          *AggMetric       `json:"max,omitempty"`
	Avg          *AggMetric       `json:"avg,omitempty"`
	Cardinality  *AggMetric       `json:"cardinality,omitempty"`
	WeightedAvg  *AggWeightedAvg  `json:"weighted_avg,omitempty"`
	Aggs         map[string]Aggs  `json:"aggs,omitempty"`
}

type AggMetric struct {
	Field string `json:"field"`
}

type AggWeightedAvg struct {
	Value  *AggMetric `json:"value"`
	Weight *AggMetric `json:"weight"`
}

type AggTerms struct {
	Field string `json:"field"`
	Size  int    `json:"size"`
}

type AggNumericRange struct {
	Field  string         `json:"field"`
	Ranges []NumericRange `json:"ranges"`
	Keyed  bool           `json:"keyed"`
}

type NumericRange struct {
	To   float64 `json:"to"`
	From float64 `json:"from"`
}

type Sort []map[string]SortTerm

type SortTerm struct {
	Order string `json:"order"`
}
