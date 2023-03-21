// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

type IngestResponse struct {
	Took  int64         `json:"took,omitempty"`
	Error bool          `json:"error,omitempty"`
	Items []*IngestItem `json:"items,omitempty"`
}

type IngestItem struct {
	Index       string `json:"_index"`
	Type        string `json:"_type"`
	ID          string `json:"_id"`
	Version     string `json:"_version"`
	Shards      Shards `json:"_shards"`
	SeqNo       int64  `json:"_seq_no"`
	PrimaryTerm int    `json:"_primary_term"`
}

type IngestRequest struct {
	Index     string     `json:"index"`
	Documents []Document `json:"documents"`
}

type BulkAction map[string]*BulkMeta

type BulkMeta struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
}
