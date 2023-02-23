// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package aggregations contains custom aggregation for bluge
package aggregations

import (
	"math"
	"sort"
	"strconv"

	"github.com/blugelabs/bluge/search"
	"github.com/blugelabs/bluge/search/aggregations"
	"github.com/tatris-io/tatris/internal/indexlib"
)

// Reference
// https://github.com/blugelabs/bluge/blob/master/search/aggregations/terms.go
// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-histogram-aggregation.html#_missing_value_2

type HistogramAggregation struct {
	src            search.NumericValuesSource
	interval       float64
	minDocCount    int
	offset         float64
	extendedBounds *indexlib.HistogramBound
	hardBounds     *indexlib.HistogramBound
	aggregations   map[string]search.Aggregation
	lessFunc       func(a, b *search.Bucket) bool
	desc           bool
	sortFunc       func(p sort.Interface)
}

func NewHistogramAggregation(
	field search.NumericValuesSource,
	interval float64,
	offset float64,
	minDocCount int,
	extendedBounds,
	hardBounds *indexlib.HistogramBound,
) *HistogramAggregation {
	rv := &HistogramAggregation{
		src:            field,
		interval:       interval,
		minDocCount:    minDocCount,
		offset:         offset,
		extendedBounds: extendedBounds,
		hardBounds:     hardBounds,
		desc:           false,
		lessFunc: func(a, b *search.Bucket) bool {
			fa, _ := strconv.ParseFloat(a.Name(), 64)
			fb, _ := strconv.ParseFloat(b.Name(), 64)
			return fa < fb
		},
		aggregations: make(map[string]search.Aggregation),
		sortFunc:     sort.Sort,
	}
	rv.aggregations["count"] = aggregations.CountMatches()
	return rv
}

func (d *HistogramAggregation) Fields() []string {
	rv := d.src.Fields()
	for _, agg := range d.aggregations {
		rv = append(rv, agg.Fields()...)
	}
	return rv
}

func (d *HistogramAggregation) AddAggregation(name string, aggregation search.Aggregation) {
	d.aggregations[name] = aggregation
}

func (d *HistogramAggregation) Calculator() search.Calculator {
	return &HistogramCalculator{
		src:            d.src,
		interval:       d.interval,
		minDocCount:    d.minDocCount,
		offset:         d.offset,
		extendedBounds: d.extendedBounds,
		hardBounds:     d.hardBounds,
		minValue:       math.MaxInt64,
		maxValue:       math.MinInt64,
		aggregations:   d.aggregations,
		desc:           d.desc,
		lessFunc:       d.lessFunc,
		sortFunc:       d.sortFunc,
		bucketsMap:     make(map[string]*search.Bucket),
	}
}

type HistogramCalculator struct {
	src            search.NumericValuesSource
	interval       float64
	minDocCount    int
	offset         float64
	extendedBounds *indexlib.HistogramBound
	hardBounds     *indexlib.HistogramBound
	minValue       float64
	maxValue       float64
	aggregations   map[string]search.Aggregation
	bucketsList    []*search.Bucket
	bucketsMap     map[string]*search.Bucket
	total          int
	other          int
	desc           bool
	lessFunc       func(a, b *search.Bucket) bool
	sortFunc       func(p sort.Interface)
}

func (c *HistogramCalculator) Consume(d *search.DocumentMatch) {
	c.total++
	for _, value := range c.src.Numbers(d) {
		// need to filter out the ones that aren't in the hard_bounds range
		if hb := c.hardBounds; hb != nil {
			if value < hb.Min || value > hb.Max {
				return
			}
		}

		if value < c.minValue {
			c.minValue = value
		}
		if value > c.maxValue {
			c.maxValue = value
		}

		key := c.calculatorBucketKey(value)
		bucket, ok := c.bucketsMap[key]
		if ok {
			bucket.Consume(d)
		} else {
			newBucket := search.NewBucket(key, c.aggregations)
			newBucket.Consume(d)
			c.bucketsMap[key] = newBucket
			c.bucketsList = append(c.bucketsList, newBucket)
		}
	}
}

func (c *HistogramCalculator) Merge(other search.Calculator) {
	if other, ok := other.(*HistogramCalculator); ok {
		c.total += other.total
		for i := range other.bucketsList {
			var foundLocal bool
			for j := range c.bucketsList {
				if other.bucketsList[i].Name() == c.bucketsList[j].Name() {
					c.bucketsList[j].Merge(other.bucketsList[i])
					foundLocal = true
				}
			}
			if !foundLocal {
				c.bucketsList = append(c.bucketsList, other.bucketsList[i])
			}
		}
		c.Finish()
	}
}

func (c *HistogramCalculator) Finish() {
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-histogram-aggregation.html#search-aggregations-bucket-histogram-aggregation-extended-bounds
	// extended_bounds is not filtering buckets
	if eb := c.extendedBounds; eb != nil {
		if min := eb.Min; min < c.minValue {
			c.minValue = min
		}
		if max := eb.Max; max > c.maxValue {
			c.maxValue = max
		}
	}
	// hard_bounds maybe filtering buckets
	if hb := c.hardBounds; hb != nil {
		c.minValue = hb.Min
		c.maxValue = hb.Max
	}

	// zero filling
	if c.minDocCount == 0 {
		for date := c.minValue; date < c.maxValue; {
			key := c.calculatorBucketKey(date)
			if _, ok := c.bucketsMap[key]; !ok {
				newBucket := search.NewBucket(key, c.aggregations)
				c.bucketsMap[key] = newBucket
				c.bucketsList = append(c.bucketsList, newBucket)
			}
			date += c.interval
		}
	} else {
		// filter bucket
		i := 0
		for _, bucket := range c.bucketsList {
			if bucket.Count() >= uint64(c.minDocCount) {
				c.bucketsList[i] = bucket
				i++
			}
		}
		c.bucketsList = c.bucketsList[:i]
	}

	if c.desc {
		c.sortFunc(sort.Reverse(c))
	} else {
		c.sortFunc(c)
	}

	var notOther int
	for _, bucket := range c.bucketsList {
		notOther += int(bucket.Aggregations()["count"].(search.MetricCalculator).Value())
	}
	c.other = c.total - notOther
}

func (c *HistogramCalculator) Buckets() []*search.Bucket {
	return c.bucketsList
}

func (c *HistogramCalculator) Other() int {
	return c.other
}

func (c *HistogramCalculator) Len() int {
	return len(c.bucketsList)
}

func (c *HistogramCalculator) Less(i, j int) bool {
	return c.lessFunc(c.bucketsList[i], c.bucketsList[j])
}

func (c *HistogramCalculator) Swap(i, j int) {
	c.bucketsList[i], c.bucketsList[j] = c.bucketsList[j], c.bucketsList[i]
}

func (c *HistogramCalculator) calculatorBucketKey(value float64) string {
	bucketKey := math.Floor((value-c.offset)/c.interval)*c.interval + c.offset
	return strconv.FormatFloat(bucketKey, 'f', -1, 64)
}
