package it

import (
	"github.com/blugelabs/bluge/analysis"
)

// this content was obtained from:
// lucene-4.7.2/analysis/common/src/resources/org/apache/lucene/analysis

var ItalianArticles = []byte(`
c
l
all
dall
dell
nell
sull
coll
pell
gl
agl
dagl
degl
negl
sugl
un
m
t
s
v
d
`)

func Articles() analysis.TokenMap {
	rv := analysis.NewTokenMap()
	rv.LoadBytes(ItalianArticles)
	return rv
}
