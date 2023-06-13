//  Copyright (c) 2020 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package searcher

import (
	"testing"

	"github.com/blugelabs/bluge/search/similarity"

	"github.com/blugelabs/bluge/search"
)

func TestMatchAllSearch(t *testing.T) {
	explainTrue := search.SearcherOptions{Explain: true}

	allSearcher, err := NewMatchAllSearcher(baseTestIndexReader, 1.0, similarity.ConstantScorer(1.0), explainTrue)
	if err != nil {
		t.Fatal(err)
	}

	allSearcher2, err := NewMatchAllSearcher(baseTestIndexReader, 1.2, similarity.ConstantScorer(1.0), explainTrue)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		searcher  search.Searcher
		queryNorm float64
		results   []*search.DocumentMatch
	}{
		{
			searcher: allSearcher,
			results: []*search.DocumentMatch{
				{
					Number: baseTestIndexReaderDirect.docNumByID("1"),
					Score:  1.0,
				},
				{
					Number: baseTestIndexReaderDirect.docNumByID("2"),
					Score:  1.0,
				},
				{
					Number: baseTestIndexReaderDirect.docNumByID("3"),
					Score:  1.0,
				},
				{
					Number: baseTestIndexReaderDirect.docNumByID("4"),
					Score:  1.0,
				},
				{
					Number: baseTestIndexReaderDirect.docNumByID("5"),
					Score:  1.0,
				},
			},
		},
		{
			searcher: allSearcher2,
			results: []*search.DocumentMatch{
				{
					Number: baseTestIndexReaderDirect.docNumByID("1"),
					Score:  1.0,
				},
				{
					Number: baseTestIndexReaderDirect.docNumByID("2"),
					Score:  1.0,
				},
				{
					Number: baseTestIndexReaderDirect.docNumByID("3"),
					Score:  1.0,
				},
				{
					Number: baseTestIndexReaderDirect.docNumByID("4"),
					Score:  1.0,
				},
				{
					Number: baseTestIndexReaderDirect.docNumByID("5"),
					Score:  1.0,
				},
			},
		},
	}

	for testIndex, test := range tests {
		defer func() {
			err := test.searcher.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()

		ctx := &search.Context{
			DocumentMatchPool: search.NewDocumentMatchPool(test.searcher.DocumentMatchPoolSize(), 0),
		}
		next, err := test.searcher.Next(ctx)
		i := 0
		for err == nil && next != nil {
			if i < len(test.results) {
				if next.Number != test.results[i].Number {
					t.Errorf("expected result %d to have number %d got %d for test %d", i, test.results[i].Number, next.Number, testIndex)
				}
				if !scoresCloseEnough(next.Score, test.results[i].Score) {
					t.Errorf("expected result %d to have score %v got  %v for test %d", i, test.results[i].Score, next.Score, testIndex)
					t.Logf("scoring explanation: %s", next.Explanation)
				}
			}
			ctx.DocumentMatchPool.Put(next)
			next, err = test.searcher.Next(ctx)
			i++
		}
		if err != nil {
			t.Fatalf("error iterating searcher: %v for test %d", err, testIndex)
		}
		if len(test.results) != i {
			t.Errorf("expected %d results got %d for test %d", len(test.results), i, testIndex)
		}
	}
}
