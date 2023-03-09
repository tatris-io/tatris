// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

import (
	"time"

	"github.com/tatris-io/tatris/internal/protocol"
)

type QueryResponse struct {
	Took         int64                  `json:"took"`
	Hits         Hits                   `json:"hits"`
	Aggregations map[string]Aggregation `json:"aggregations"`
}

type Hits struct {
	Total Total `json:"total"`
	Hits  []Hit `json:"hits"`
}

type Total struct {
	Value    int64  `json:"value"`
	Relation string `json:"relation"`
}

type Hit struct {
	Index     string            `json:"_index"`
	ID        string            `json:"_id"`
	Source    protocol.Document `json:"_source"`
	Timestamp time.Time         `json:"_timestamp"`
}

type Aggregation struct {
	Type    string            `json:"_type"`             // real type of Aggregation, see: internal/common/consts/aggregation.go
	Value   interface{}       `json:"value,omitempty"`   // metric aggregation
	Buckets []protocol.Bucket `json:"buckets,omitempty"` // bucket aggregation
}
