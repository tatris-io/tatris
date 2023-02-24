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
	Terms         *AggTerms         `json:"terms,omitempty"`
	DateHistogram *AggDateHistogram `json:"date_histogram,omitempty"`
	Histogram     *AggHistogram     `json:"histogram,omitempty"`
	NumericRange  *AggNumericRange  `json:"range"`
	Sum           *AggMetric        `json:"sum,omitempty"`
	Min           *AggMetric        `json:"min,omitempty"`
	Max           *AggMetric        `json:"max,omitempty"`
	Avg           *AggMetric        `json:"avg,omitempty"`
	Cardinality   *AggMetric        `json:"cardinality,omitempty"`
	Percentiles   *AggPercentiles   `json:"percentiles,omitempty"`
	WeightedAvg   *AggWeightedAvg   `json:"weighted_avg,omitempty"`
	Aggs          map[string]Aggs   `json:"aggs,omitempty"`
}

type AggMetric struct {
	Field string `json:"field"`
}

type AggPercentiles struct {
	Field    string    `json:"field"` // only support numeric type
	Percents []float64 `json:"percents"`
	// Approximate algorithms must balance memory utilization with estimation accuracy. This balance
	// can be controlled using a compression parameter
	Compression float64 `json:"compression"`
}

type AggWeightedAvg struct {
	Value  *AggMetric `json:"value"`
	Weight *AggMetric `json:"weight"`
}

type AggTerms struct {
	Field string `json:"field"`
	// This is to handle the case when one term has many documents on one shard but is just below
	// the size threshold on all other shards. If each shard only returned size terms, the
	// aggregation would return an partial doc count for the term. So terms returns more terms in an
	// attempt to catch the missing terms. This helps, but it’s still quite possible to return a
	// partial doc count for a term.
	// It just takes a term with more disparate per-shard doc counts.
	Size int `json:"size"` // default 10
	// increase shard_size to better account for these disparate doc counts and improve the accuracy
	// of the selection of top terms. It is much cheaper to increase the shard_size than to increase
	// the size. However, it still takes more bytes over the wire and
	// waiting in memory on the coordinating node.
	ShardSize int `json:"shard_size"` // default 5000
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
	Order   string `json:"order"`
	Missing string `json:"missing"`
}

type AggDateHistogram struct {
	Field            string      `json:"field"`             // only support date type
	Interval         string      `json:"interval"`          // combined interval field is deprecated since elasticsearch7.x
	FixedInterval    string      `json:"fixed_interval"`    // milliseconds (ms)/seconds (s)/minutes (m)/hours (h)/days (d)
	CalendarInterval string      `json:"calendar_interval"` // minute, 1m/hour, 1h/day, 1d/week, 1w/month, 1M/quarter, 1q/year, 1y
	TimeZone         string      `json:"time_zone"`         // +0100/+01:00, -0100/-01:00, UTC, Asia/Shanghai...
	MinDocCount      int         `json:"min_doc_count"`     // min_doc_count: 0, Zero filling
	Format           string      `json:"format"`            // TODO
	Offset           string      `json:"offset"`            // TODO
	Keyed            bool        `json:"keyed"`             // TODO
	Order            interface{} `json:"order"`             // TODO
	Missing          string      `json:"missing"`           // TODO
	// With extended_bounds setting, you now can "force" the histogram aggregation to start building
	// buckets on a specific min value and also keep on building buckets up to a max value (even if
	// there are no documents anymore). Using extended_bounds only makes sense when min_doc_count is
	// 0 (the empty buckets will never be returned if min_doc_count is greater than 0).
	ExtendedBounds *HistogramBound `json:"extended_bounds"`
	// The hard_bounds is a counterpart of extended_bounds and can limit the range of buckets in the
	// histogram. It is particularly useful in the case of open data ranges that can result in a
	// very large number of buckets.
	HardBounds *HistogramBound `json:"hard_bounds"`
}

type AggHistogram struct {
	Field          string          `json:"field"` // only support numeric type
	Interval       float64         `json:"interval"`
	MinDocCount    int             `json:"min_doc_count"`
	Offset         float64         `json:"offset"`
	Keyed          bool            `json:"keyed"`   // TODO
	Order          interface{}     `json:"order"`   // TODO
	Missing        string          `json:"missing"` // TODO
	ExtendedBounds *HistogramBound `json:"extended_bounds"`
	HardBounds     *HistogramBound `json:"hard_bounds"`
}

type HistogramBound struct {
	Min float64 `json:"min"`
	Max float64 `json:"max"`
}
