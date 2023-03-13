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
	Total    Total   `json:"total"`
	Hits     []Hit   `json:"hits"`
	MaxScore float64 `json:"max_score"`
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
	Score     float64           `json:"_score"`
	Type      string            `json:"_type"`
}

type Aggregation struct {
	Value   interface{}       `json:"value,omitempty"`   // metric aggregation
	Buckets []protocol.Bucket `json:"buckets,omitempty"` // bucket aggregation
}
