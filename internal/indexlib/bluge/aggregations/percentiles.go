// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package aggregations

import (
	"strconv"

	"github.com/blugelabs/bluge/search"
	"github.com/caio/go-tdigest"
)

type PercentilesMetric struct {
	src         search.NumericValuesSource
	percents    []float64
	compression float64
}

func NewPercentiles(
	src search.NumericValuesSource,
	percents []float64,
	compression float64,
) *PercentilesMetric {
	return &PercentilesMetric{
		src:         src,
		percents:    percents,
		compression: compression,
	}
}

func (c *PercentilesMetric) Fields() []string {
	return c.src.Fields()
}

func (c *PercentilesMetric) Calculator() search.Calculator {
	rv := &PercentilesCalculator{
		src:      c.src,
		percents: c.percents,
	}
	rv.tdigest, _ = tdigest.New(tdigest.Compression(c.compression))
	return rv
}

type PercentilesCalculator struct {
	src      search.NumericValuesSource
	percents []float64
	tdigest  *tdigest.TDigest
}

func (c *PercentilesCalculator) Value() map[string]float64 {
	result := make(map[string]float64, len(c.percents))
	for _, percent := range c.percents {
		key := strconv.FormatFloat(percent, 'f', -1, 64)
		result[key] = c.tdigest.Quantile(percent / 100)
	}

	return result
}

func (c *PercentilesCalculator) Consume(d *search.DocumentMatch) {
	for _, val := range c.src.Numbers(d) {
		_ = c.tdigest.Add(val)
	}
}

func (c *PercentilesCalculator) Merge(other search.Calculator) {
	if other, ok := other.(*PercentilesCalculator); ok {
		_ = c.tdigest.Merge(other.tdigest)
	}
}

func (c *PercentilesCalculator) Finish() {

}
