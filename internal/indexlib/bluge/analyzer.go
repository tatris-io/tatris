// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package bluge

import (
	"strings"

	"github.com/blugelabs/bluge/analysis"
	"github.com/blugelabs/bluge/analysis/analyzer"
)

func genAnalyzer(analyzerStr string) *analysis.Analyzer {
	switch strings.ToUpper(analyzerStr) {
	case "KEYWORD":
		return analyzer.NewKeywordAnalyzer()
	case "SIMPLE":
		return analyzer.NewSimpleAnalyzer()
	case "STANDARD":
		return analyzer.NewStandardAnalyzer()
	case "WEB":
		return analyzer.NewWebAnalyzer()
	default:
		return analyzer.NewStandardAnalyzer()
	}
}
