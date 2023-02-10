// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package bluge

import (
	"math"
	"strconv"

	"github.com/blugelabs/bluge"
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/indexlib"
)

func RangeQueryParse(rangeQuery *indexlib.RangeQuery) (bluge.Query, error) {
	if len(rangeQuery.Range) <= 0 {
		return nil, &errs.InvalidQueryError{Query: rangeQuery, Message: "invalid range"}
	}
	//numeric parse
	field := ""
	min := float64(math.MinInt64)
	max := float64(math.MaxInt64)
	containsMin := false
	containsMax := false

	for k, v := range rangeQuery.Range {
		field = k
		if v.GT != nil {
			min, _ = toFloat64(v.GT)
		} else if v.GTE != nil {
			min, _ = toFloat64(v.GTE)
			containsMin = true
		}
		if v.LT != nil {
			max, _ = toFloat64(v.LT)
		} else if v.LTE != nil {
			max, _ = toFloat64(v.LTE)
			containsMax = true
		}
	}
	q := bluge.NewNumericRangeInclusiveQuery(min, max, containsMin, containsMax).SetField(field)
	return q, nil
}

func toFloat64(v interface{}) (float64, error) {
	switch v := v.(type) {
	case float64:
		return v, nil
	case uint64:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, &errs.UnsupportedError{Desc: "range type", Value: v}
	}
}
