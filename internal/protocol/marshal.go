// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

import (
	"encoding/json"
	"reflect"

	"github.com/tidwall/gjson"
)

func (s *Settings) UnmarshalJSON(data []byte) error {
	var err error
	result := gjson.ParseBytes(data)
	numberOfShards := result.Get("number_of_shards")
	if !numberOfShards.Exists() {
		numberOfShards = result.Get("index.number_of_shards")
	}
	if numberOfShards.Exists() {
		s.NumberOfShards = int(numberOfShards.Int())
	}

	numberOfReplicas := result.Get("number_of_replicas")
	if !numberOfReplicas.Exists() {
		numberOfReplicas = result.Get("index.number_of_replicas")
	}
	if numberOfReplicas.Exists() {
		s.NumberOfReplicas = int(numberOfReplicas.Int())
	}
	return err
}

func (q *QueryRequest) UnmarshalJSON(data []byte) error {
	var err error
	tmp := struct {
		Index     string          `json:"index"`
		Query     Query           `json:"query"`
		Aggs      map[string]Aggs `json:"aggs"`
		Sort      Sort            `json:"sort"`
		Size      int64           `json:"size"`
		From      int64           `json:"from"`
		TypedKeys bool            `json:"typed_keys"`
	}{}
	if err = json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	// Any one of `agg` or `aggregations` can be specified to represent aggregated queries.
	// This is for compatibility with elasticsearch's query protocol.
	if tmp.Aggs == nil {
		result := gjson.ParseBytes(data)
		aggregations := result.Get("aggregations")
		if aggregations.Exists() {
			tmp.Aggs = make(map[string]Aggs)
			err = json.Unmarshal([]byte(aggregations.Raw), &tmp.Aggs)
		}
	}
	if !reflect.ValueOf(tmp.Index).IsZero() {
		q.Index = tmp.Index
	}
	if !reflect.ValueOf(tmp.Query).IsZero() {
		q.Query = tmp.Query
	}
	if !reflect.ValueOf(tmp.Aggs).IsZero() {
		q.Aggs = tmp.Aggs
	}
	if !reflect.ValueOf(tmp.Sort).IsZero() {
		q.Sort = tmp.Sort
	}
	if reflect.ValueOf(tmp.Size).Int() >= 0 {
		q.Size = tmp.Size
	}
	if !reflect.ValueOf(tmp.From).IsZero() {
		q.From = tmp.From
	}
	if !reflect.ValueOf(tmp.TypedKeys).IsZero() {
		q.TypedKeys = tmp.TypedKeys
	}
	return err
}

func (q *Aggs) UnmarshalJSON(data []byte) error {
	var err error
	tmp := struct {
		Terms         *AggTerms         `json:"terms,omitempty"`
		DateHistogram *AggDateHistogram `json:"date_histogram,omitempty"`
		Histogram     *AggHistogram     `json:"histogram,omitempty"`
		NumericRange  *AggNumericRange  `json:"range"`
		DateRange     *AggDateRange     `json:"date_range"`
		Filter        *Query            `json:"filter"`
		Count         *AggMetric        `json:"count,omitempty"`
		ValueCount    *AggMetric        `json:"value_count,omitempty"`
		Sum           *AggMetric        `json:"sum,omitempty"`
		Min           *AggMetric        `json:"min,omitempty"`
		Max           *AggMetric        `json:"max,omitempty"`
		Avg           *AggMetric        `json:"avg,omitempty"`
		Cardinality   *AggMetric        `json:"cardinality,omitempty"`
		Percentiles   *AggPercentiles   `json:"percentiles,omitempty"`
		WeightedAvg   *AggWeightedAvg   `json:"weighted_avg,omitempty"`
		Aggs          map[string]Aggs   `json:"aggs,omitempty"`
	}{}
	if err = json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	// Any one of `agg` or `aggregations` can be specified to represent aggregated queries.
	// This is for compatibility with elasticsearch's query protocol.
	if tmp.Aggs == nil {
		result := gjson.ParseBytes(data)
		aggregations := result.Get("aggregations")
		if aggregations.Exists() {
			tmp.Aggs = make(map[string]Aggs)
			err = json.Unmarshal([]byte(aggregations.Raw), &tmp.Aggs)
		}
	}
	q.Terms = tmp.Terms
	q.DateHistogram = tmp.DateHistogram
	q.Histogram = tmp.Histogram
	q.NumericRange = tmp.NumericRange
	q.DateRange = tmp.DateRange
	q.Filter = tmp.Filter
	q.Count = tmp.Count
	q.ValueCount = tmp.ValueCount
	q.Sum = tmp.Sum
	q.Min = tmp.Min
	q.Max = tmp.Max
	q.Avg = tmp.Avg
	q.Cardinality = tmp.Cardinality
	q.Percentiles = tmp.Percentiles
	q.WeightedAvg = tmp.WeightedAvg
	q.Aggs = tmp.Aggs
	return err
}

func (r *RangeVal) UnmarshalJSON(data []byte) error {
	var tmp struct {
		Gt  interface{} `json:"gt,omitempty"`
		Gte interface{} `json:"gte,omitempty"`
		Lt  interface{} `json:"lt,omitempty"`
		Lte interface{} `json:"lte,omitempty"`
	}
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}
	// Range queries may be expressed in two ways:
	// {gt, gte, lt, lte} OR
	// {from, include_lower, to, include_upper}.
	// This is for compatibility with elasticsearch's query protocol.
	if tmp.Lte == nil && tmp.Lt == nil && tmp.Gte == nil && tmp.Gt == nil {
		result := gjson.ParseBytes(data)
		from := result.Get("from")
		if from.Exists() {
			includeLower := result.Get("include_lower")
			if includeLower.Exists() && includeLower.Bool() {
				tmp.Gte = from.Value()
			} else {
				tmp.Gt = from.Value()
			}
		}
		to := result.Get("to")
		if to.Exists() {
			includeUpper := result.Get("include_upper")
			if includeUpper.Exists() && includeUpper.Bool() {
				tmp.Lte = to.Value()
			} else {
				tmp.Lt = to.Value()
			}
		}
	}
	r.Gt = tmp.Gt
	r.Gte = tmp.Gte
	r.Lt = tmp.Lt
	r.Lte = tmp.Lte
	return nil
}
