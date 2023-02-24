// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package bluge

import (
	"math"
	"time"

	"github.com/blugelabs/bluge"
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/common/utils"
	"github.com/tatris-io/tatris/internal/indexlib"
)

func ParseRangeQuery(rangeQuery *indexlib.RangeQuery) (bluge.Query, error) {
	if len(rangeQuery.Range) <= 0 {
		return nil, &errs.InvalidQueryError{Query: rangeQuery, Message: "invalid range"}
	}
	containsMin := false
	containsMax := false
	for field, rangeVal := range rangeQuery.Range {
		if utils.IsNumeric(rangeVal.GT) || utils.IsNumeric(rangeVal.GTE) ||
			utils.IsNumeric(rangeVal.LT) ||
			utils.IsNumeric(rangeVal.LTE) {
			min := float64(math.MinInt64)
			max := float64(math.MaxInt64)
			if utils.IsNumeric(rangeVal.GT) {
				min, _ = utils.ToFloat64(rangeVal.GT)
			}
			if utils.IsNumeric(rangeVal.GTE) {
				min, _ = utils.ToFloat64(rangeVal.GTE)
				containsMin = true
			}
			if utils.IsNumeric(rangeVal.LT) {
				max, _ = utils.ToFloat64(rangeVal.LT)
			}
			if utils.IsNumeric(rangeVal.LTE) {
				max, _ = utils.ToFloat64(rangeVal.LTE)
				containsMax = true
			}
			return bluge.NewNumericRangeInclusiveQuery(min, max, containsMin, containsMax).
				SetField(field), nil

		}
		if utils.IsDateType(rangeVal.GT) || utils.IsDateType(rangeVal.GTE) ||
			utils.IsDateType(rangeVal.LT) ||
			utils.IsDateType(rangeVal.LTE) {
			min := time.UnixMilli(0)
			max := time.Now()
			if utils.IsDateType(rangeVal.GT) {
				min, _ = utils.ParseTime(rangeVal.GT)
			}
			if utils.IsDateType(rangeVal.GTE) {
				min, _ = utils.ParseTime(rangeVal.GTE)
				containsMin = true
			}
			if utils.IsDateType(rangeVal.LT) {
				max, _ = utils.ParseTime(rangeVal.LT)
			}
			if utils.IsDateType(rangeVal.LTE) {
				max, _ = utils.ParseTime(rangeVal.LTE)
				containsMax = true
			}
			return bluge.NewDateRangeInclusiveQuery(min, max, containsMin, containsMax).
					SetField(field),
				nil
		}
		if utils.IsString(rangeVal.GT) || utils.IsString(rangeVal.GTE) ||
			utils.IsString(rangeVal.LT) ||
			utils.IsString(rangeVal.LTE) {
			var min, max string
			if utils.IsString(rangeVal.GT) {
				min = utils.ToString(rangeVal.GT)
			}
			if utils.IsString(rangeVal.GTE) {
				min = utils.ToString(rangeVal.GTE)
				containsMin = true
			}
			if utils.IsString(rangeVal.LT) {
				max = utils.ToString(rangeVal.LT)
			}
			if utils.IsString(rangeVal.LTE) {
				max = utils.ToString(rangeVal.LTE)
				containsMax = true
			}
			return bluge.NewTermRangeInclusiveQuery(min, max, containsMin, containsMax).
					SetField(field),
				nil
		}
	}
	return nil, &errs.InvalidQueryError{Message: "range query", Query: rangeQuery}
}
