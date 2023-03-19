// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package protocol describes the core data structures and calling conventions of Tatris
package protocol

// QueryRequest provides a full query DSL based on JSON to define queries.
// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/query-dsl.html
// The JSON unmarshalling of QueryRequest is redefined in UnmarshalJSON.
type QueryRequest struct {
	Index string          `json:"index"`
	Query Query           `json:"query"`
	Aggs  map[string]Aggs `json:"aggs"`
	Sort  Sort            `json:"sort"`
	Size  int64           `json:"size"`
	From  int64           `json:"from"`
	// When the parameter typed_keys is true will be prefixing aggregation names with their type
	TypedKeys bool `json:"typed_keys"`
}

// Query describes how to search the document
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

// MatchAll matches all documents.
// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/query-dsl-match-all-query.html
type MatchAll struct{}

// Match returns documents that match a provided text, number, date or boolean value. The provided
// text is analyzed before matching.
// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/query-dsl-match-query.html
type Match map[string]interface{}

// MatchPhrase analyzes the text and creates a phrase query out of the analyzed text.
// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/query-dsl-match-query-phrase.html
type MatchPhrase map[string]interface{}

// QueryString returns documents based on a provided query string, using a parser with a strict
// syntax.
// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/query-dsl-query-string-query.html
type QueryString map[string]interface{}

// Ids returns documents based on their IDs.
// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/query-dsl-ids-query.html
type Ids struct {
	Values []string `json:"values"`
}

// Term returns documents that contain an exact term in a provided field.
// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/query-dsl-term-query.html
type Term map[string]interface{}

// Terms returns documents that contain one or more exact terms in a provided field.
// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/query-dsl-terms-query.html
type Terms map[string]interface{}

// Range returns documents that contain terms within a provided range.
// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/query-dsl-range-query.html
type Range map[string]*RangeVal

// RangeVal describes a filter interval.
// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/query-dsl-range-query.html
// The JSON unmarshalling of RangeVal is redefined in UnmarshalJSON.
type RangeVal struct {
	Gt  interface{} `json:"gt,omitempty"`
	Gte interface{} `json:"gte,omitempty"`
	Lt  interface{} `json:"lt,omitempty"`
	Lte interface{} `json:"lte,omitempty"`
}

// Bool matches documents matching boolean combinations of other queries.
// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/query-dsl-bool-query.html
type Bool struct {
	Must               []*Query `json:"must,omitempty"`
	MustNot            []*Query `json:"must_not,omitempty"`
	Should             []*Query `json:"should,omitempty"`
	Filter             []*Query `json:"filter,omitempty"`
	MinimumShouldMatch string   `json:"minimum_should_match,omitempty"`
}

// Aggs summarizes the data as metrics, statistics, or other analytics.
// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations.html
// The JSON unmarshalling of Aggs is redefined in UnmarshalJSON.
type Aggs struct {

	// Terms is a multi-bucket value source based aggregation where buckets are dynamically built -
	// one per unique value.
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations-bucket-terms-aggregation.html
	Terms *AggTerms `json:"terms,omitempty"`
	// DateHistogram is similar to the normal Histogram, but it can only be used with date or date
	// range values.
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations-bucket-datehistogram-aggregation.html
	DateHistogram *AggDateHistogram `json:"date_histogram,omitempty"`
	// Histogram is a multi-bucket values source based aggregation that can be applied on numeric
	// values or numeric range values extracted from the documents.
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations-bucket-histogram-aggregation.html
	Histogram *AggHistogram `json:"histogram,omitempty"`
	// NumericRange is a multi-bucket value source based aggregation that enables the user to define
	// a set of ranges - each representing a bucket.
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations-bucket-range-aggregation.html
	NumericRange *AggNumericRange `json:"range"`
	// DateRange is a range aggregation that is dedicated for date values.
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations-bucket-daterange-aggregation.html
	DateRange *AggDateRange `json:"date_range"`
	// Filter is a single bucket aggregation that narrows the set of documents to those that match a
	// Query.
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations-bucket-filter-aggregation.html
	Filter *Query `json:"filter"`
	// Count or ValueCount is a single-value metrics aggregation that counts the number of values
	// that are extracted from the aggregated documents.
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations-metrics-valuecount-aggregation.html
	Count      *AggMetric `json:"count,omitempty"`
	ValueCount *AggMetric `json:"value_count,omitempty"`
	// Sum is a single-value metrics aggregation that sums up numeric values that are extracted from
	// the aggregated documents.
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations-metrics-sum-aggregation.html
	Sum *AggMetric `json:"sum,omitempty"`
	// Min is a single-value metrics aggregation that keeps track and returns the minimum value
	// among numeric values extracted from the aggregated documents.
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations-metrics-min-aggregation.html
	Min *AggMetric `json:"min,omitempty"`
	// Max is a single-value metrics aggregation that keeps track and returns the maximum value
	// among the numeric values extracted from the aggregated documents.
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations-metrics-max-aggregation.html
	Max *AggMetric `json:"max,omitempty"`
	// Avg is a single-value metrics aggregation that computes the average of numeric values that
	// are extracted from the aggregated documents.
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations-metrics-avg-aggregation.html
	Avg *AggMetric `json:"avg,omitempty"`
	// Cardinality is a single-value metrics aggregation that calculates an approximate count of
	// distinct values.
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations-metrics-valuecount-aggregation.html
	Cardinality *AggMetric `json:"cardinality,omitempty"`
	// Percentiles is a multi-value metrics aggregation that calculates one or more percentiles over
	// numeric values extracted from the aggregated documents.
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations-metrics-percentile-aggregation.html
	Percentiles *AggPercentiles `json:"percentiles,omitempty"`
	// WeightedAvg is a single-value metrics aggregation that computes the weighted average of
	// numeric values that are extracted from the aggregated documents.
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.6/search-aggregations-metrics-weight-avg-aggregation.html
	WeightedAvg *AggWeightedAvg `json:"weighted_avg,omitempty"`
	// Aggs summarizes the data as metrics, statistics, or other analytics.
	Aggs map[string]Aggs `json:"aggs,omitempty"`
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

type AggDateRange struct {
	Field    string      `json:"field"`
	TimeZone string      `json:"time_zone"`
	Ranges   []DateRange `json:"ranges"`
	Format   string      `json:"format"` // TODO
	Keyed    bool        `json:"keyed"`  // TODO
}

type DateRange struct {
	To   string `json:"to"`
	From string `json:"from"`
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
	Offset           any         `json:"offset"`            // TODO
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
