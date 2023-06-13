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

package highlight

import (
	"github.com/blugelabs/bluge/search"
)

// FragmentScorer will score fragments by how many
// unique terms occur in the fragment with no regard for
// any boost values used in the original query
type SimpleFragmentScorer struct {
	tlm search.TermLocationMap
}

func NewFragmentScorer(tlm search.TermLocationMap) *SimpleFragmentScorer {
	return &SimpleFragmentScorer{
		tlm: tlm,
	}
}

func (s *SimpleFragmentScorer) Score(f *Fragment) {
	score := 0.0
	for _, locations := range s.tlm {
		for _, location := range locations {
			if location.Start >= f.Start && location.End <= f.End {
				score += 1.0
				// once we find a term in the fragment
				// don't care about additional matches
				break
			}
		}
	}
	f.Score = score
}
