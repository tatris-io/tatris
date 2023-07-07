// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package bluge

import "github.com/blugelabs/bluge/search"

// An DocHeap is a min-heap of search.DocumentMatch.
type DocHeap struct {
	docs []*search.DocumentMatch
	sort search.SortOrder
}

func (h *DocHeap) Len() int           { return len(h.docs) }
func (h *DocHeap) Less(i, j int) bool { return h.sort.Compare(h.docs[i], h.docs[j]) < 0 }
func (h *DocHeap) Swap(i, j int)      { h.docs[i], h.docs[j] = h.docs[j], h.docs[i] }

func (h *DocHeap) Push(x any) {
	h.docs = append(h.docs, x.(*search.DocumentMatch))
}

func (h *DocHeap) Pop() any {
	old := *h
	n := len(old.docs)
	x := old.docs[n-1]
	h.docs = old.docs[0 : n-1]
	return x
}
