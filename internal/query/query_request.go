// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package query

// TODO: too many query type to be defined

type QueryRequest struct {
	Query Query `json:"query"`
	Size  int64 `json:"size"`
}

type Query struct {
	MatchAll *MatchAll `json:"match_all,omitempty"`
}

type MatchAll struct{}
