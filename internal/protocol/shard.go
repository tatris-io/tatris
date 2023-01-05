// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

// Shard is a logical split of the index
type Shard struct {
	ID       string    `json:"id"`
	Segments []Segment `json:"segments"`
	Stat     Stat      `json:"stat"`
}

// Segment is a physical split of the index under a shard
type Segment struct {
	ID   string `json:"id"`
	Stat Stat   `json:"stat"`
}

// Stat records the statistics of the current split
type Stat struct {
	Start  int64 `json:"start"`
	End    int64 `json:"end"`
	DocNum int64 `json:"doc_num"`
}
