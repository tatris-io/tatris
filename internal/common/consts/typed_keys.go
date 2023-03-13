// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package consts

const (
	// TypedKeysParam used in the elasticsearch sdk
	TypedKeysParam          = "typed_keys"
	TypedKeysParamValueTrue = "true"

	// TypedKeysDelimiter used when prefixing aggregation names with their type
	// using the typed_keys parameter
	TypedKeysDelimiter = "#"

	TypedKeysStermsPrefix            = "sterms"
	TypedKeysFilterPrefix            = "filter"
	TypedKeysRangePrefix             = "range"
	TypedKeysDateRangePrefix         = "date_range"
	TypedKeysDateHistogramPrefix     = "date_histogram"
	TypedKeysHistogramPrefix         = "histogram"
	TypedKeysAutoDateHistogramPrefix = "auto_date_histogram"
	TypedKeysCountPrefix             = "value_count"
	TypedKeysAvgPrefix               = "avg"
	TypedKeysSumPrefix               = "sum"
	TypedKeysMaxPrefix               = "max"
	TypedKeysMinPrefix               = "min"
	TypedKeysWeightedAvgPrefix       = "weighted_avg"
	TypedKeysCardinalityPrefix       = "cardinality"
	TypedKeysPercentilesPrefix       = "tdigest_percentiles"
)
