// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package aggregations contains custom aggregation for bluge
package aggregations

import (
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/blugelabs/bluge/search"
	"github.com/blugelabs/bluge/search/aggregations"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/indexlib"
	"go.uber.org/zap"
)

// Reference
// https://github.com/blugelabs/bluge/blob/master/search/aggregations/terms.go
// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-datehistogram-aggregation.html#search-aggregations-bucket-datehistogram-aggregation

type DateHistogramAggregation struct {
	src              search.DateValuesSource
	calendarInterval string
	fixedInterval    int64 // nanos
	minDocCount      int
	format           string
	timeZone         *time.Location
	offset           any
	extendedBounds   *indexlib.DateHistogramBound
	hardBounds       *indexlib.DateHistogramBound
	aggregations     map[string]search.Aggregation
	lessFunc         func(a, b *search.Bucket) bool
	desc             bool
	sortFunc         func(p sort.Interface)
}

func NewDateHistogramAggregation(
	field search.DateValuesSource,
	calendarInterval string,
	fixedInterval int64,
	format string,
	timeZone *time.Location,
	offset any,
	minDocCount int,
	extendedBounds,
	hardBounds *indexlib.DateHistogramBound,
) *DateHistogramAggregation {
	rv := &DateHistogramAggregation{
		src:              field,
		calendarInterval: calendarInterval,
		fixedInterval:    fixedInterval,
		minDocCount:      minDocCount,
		format:           format,
		timeZone:         timeZone,
		offset:           offset,
		extendedBounds:   extendedBounds,
		hardBounds:       hardBounds,
		desc:             false,
		lessFunc: func(a, b *search.Bucket) bool {
			return a.Name() < b.Name()
		},
		aggregations: make(map[string]search.Aggregation),
		sortFunc:     sort.Sort,
	}
	rv.aggregations["count"] = aggregations.CountMatches()
	return rv
}

func (d *DateHistogramAggregation) Fields() []string {
	rv := d.src.Fields()
	for _, agg := range d.aggregations {
		rv = append(rv, agg.Fields()...)
	}
	return rv
}

func (d *DateHistogramAggregation) AddAggregation(name string, aggregation search.Aggregation) {
	d.aggregations[name] = aggregation
}

func (d *DateHistogramAggregation) Calculator() search.Calculator {
	return &DateHistogramCalculator{
		src:              d.src,
		calendarInterval: d.calendarInterval,
		fixedInterval:    d.fixedInterval,
		minDocCount:      d.minDocCount,
		format:           d.format,
		timeZone:         d.timeZone,
		offset:           d.offset,
		extendedBounds:   d.extendedBounds,
		hardBounds:       d.hardBounds,
		minNano:          math.MaxInt64,
		maxNano:          math.MinInt64,
		aggregations:     d.aggregations,
		desc:             d.desc,
		lessFunc:         d.lessFunc,
		sortFunc:         d.sortFunc,
		bucketsMap:       make(map[string]*search.Bucket),
	}
}

type DateHistogramCalculator struct {
	src              search.DateValuesSource
	calendarInterval string
	fixedInterval    int64 // nanos
	minDocCount      int
	format           string
	timeZone         *time.Location
	offset           any
	extendedBounds   *indexlib.DateHistogramBound
	hardBounds       *indexlib.DateHistogramBound
	minNano          int64
	maxNano          int64
	aggregations     map[string]search.Aggregation
	bucketsList      []*search.Bucket
	bucketsMap       map[string]*search.Bucket
	total            int
	other            int
	desc             bool
	lessFunc         func(a, b *search.Bucket) bool
	sortFunc         func(p sort.Interface)
}

func (c *DateHistogramCalculator) Consume(d *search.DocumentMatch) {
	c.total++
	for _, date := range c.src.Dates(d) {
		// Record the maximum and minimum time used for the zero filling when min_doc_count is 0
		unixNano := date.UnixNano()

		// need to filter out the ones that aren't in the hard_bounds range
		if hb := c.hardBounds; hb != nil {
			if unixNano < hb.Min || unixNano > hb.Max {
				return
			}
		}

		if unixNano < c.minNano {
			c.minNano = unixNano
		}
		if unixNano > c.maxNano {
			c.maxNano = unixNano
		}

		key := c.calculatorBucketKey(unixNano)
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

func (c *DateHistogramCalculator) Merge(other search.Calculator) {
	if other, ok := other.(*DateHistogramCalculator); ok {
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

func (c *DateHistogramCalculator) Finish() {
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-histogram-aggregation.html#search-aggregations-bucket-histogram-aggregation-extended-bounds
	// extended_bounds is not filtering buckets
	if eb := c.extendedBounds; eb != nil {
		if min := eb.Min; min < c.minNano {
			c.minNano = min
		}
		if max := eb.Max; max > c.maxNano {
			c.maxNano = max
		}
	}
	// hard_bounds maybe filtering buckets
	if hb := c.hardBounds; hb != nil {
		c.minNano = hb.Min
		c.maxNano = hb.Max
	}

	// zero filling
	if c.minDocCount == 0 {
		for date := c.minNano; date < c.maxNano; {
			key := c.calculatorBucketKey(date)
			if _, ok := c.bucketsMap[key]; !ok {
				newBucket := search.NewBucket(key, c.aggregations)
				c.bucketsMap[key] = newBucket
				c.bucketsList = append(c.bucketsList, newBucket)
			}
			date = c.calculatorNextBucketKey(date)
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

func (c *DateHistogramCalculator) Buckets() []*search.Bucket {
	return c.bucketsList
}

func (c *DateHistogramCalculator) Other() int {
	return c.other
}

func (c *DateHistogramCalculator) Len() int {
	return len(c.bucketsList)
}

func (c *DateHistogramCalculator) Less(i, j int) bool {
	return c.lessFunc(c.bucketsList[i], c.bucketsList[j])
}

func (c *DateHistogramCalculator) Swap(i, j int) {
	c.bucketsList[i], c.bucketsList[j] = c.bucketsList[j], c.bucketsList[i]
}

func (c *DateHistogramCalculator) calculatorBucketKey(unixNano int64) string {
	var ms int64
	t := time.Unix(0, unixNano)
	if c.timeZone != nil {
		_, offset := t.In(c.timeZone).Zone()
		t = t.Add(time.Duration(offset) * time.Second)
	}

	if c.calendarInterval != "" {
		switch c.calendarInterval {
		case "minute", "1m":
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
		case "hour", "1h":
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
		case "day", "1d":
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
		case "week", "1w":
			t = time.Date(t.Year(), t.Month(), t.Day()-int(t.Weekday()), 0, 0, 0, 0, t.Location())
		case "month", "1M":
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location())
		case "quarter", "1q":
			switch t.Month() {
			case 1, 2, 3:
				t = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
			case 4, 5, 6:
				t = time.Date(t.Year(), 4, 1, 0, 0, 0, 0, t.Location())
			case 7, 8, 9:
				t = time.Date(t.Year(), 7, 1, 0, 0, 0, 0, t.Location())
			case 10, 11, 12:
				t = time.Date(t.Year(), 10, 1, 0, 0, 0, 0, t.Location())
			}
		case "year", "1y":
			t = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location())
		default:
			logger.Error(
				"date_histogram calendar_interval not support",
				zap.String("calendar_interval", c.calendarInterval),
			)
		}
		ms = t.UnixMilli()
	} else {
		ms = (t.UnixNano() / c.fixedInterval) * c.fixedInterval / int64(time.Millisecond)
	}

	return strconv.FormatInt(ms, 10)
}

func (c *DateHistogramCalculator) calculatorNextBucketKey(unixNano int64) int64 {
	var nanos int64
	t := time.Unix(0, unixNano)
	if c.timeZone != nil {
		_, offset := t.In(c.timeZone).Zone()
		t = t.Add(time.Duration(offset) * time.Second)
	}

	if c.calendarInterval != "" {
		switch c.calendarInterval {
		case "minute", "1m":
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()+1, 0, 0, t.Location())
		case "hour", "1h":
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
		case "day", "1d":
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
		case "week", "1w":
			t = time.Date(t.Year(), t.Month(), t.Day()+7, 0, 0, 0, 0, t.Location())
		case "month", "1M":
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
		case "quarter", "1q":
			t = time.Date(t.Year(), t.Month()+3, 1, 0, 0, 0, 0, t.Location())
		case "year", "1y":
			t = time.Date(t.Year()+1, 1, 1, 0, 0, 0, 0, t.Location())
		default:
			logger.Error(
				"date_histogram calendar_interval not support",
				zap.String("calendar_interval", c.calendarInterval),
			)
		}
		nanos = t.UnixNano()
	} else {
		nanos = unixNano + c.fixedInterval
	}

	return nanos
}
