// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

type QueryResponse struct {
	Took         int64                   `json:"took"` // unit: ms
	TimedOut     bool                    `json:"timed_out"`
	Shards       Shards                  `json:"_shards"`
	Hits         Hits                    `json:"hits"`
	Error        interface{}             `json:"error,omitempty"`
	Status       int32                   `json:"status"`
	Aggregations map[string]AggsResponse `json:"aggregations,omitempty"`
}

type Shards struct {
	Total      int32 `json:"total"`
	Successful int32 `json:"successful"`
	Skipped    int32 `json:"skipped"`
	Failed     int32 `json:"failed"`
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
	Index  string   `json:"_index"`
	ID     string   `json:"_id"`
	Source Document `json:"_source"`
}

type AggsResponse struct {
	// Type is used to distinguish how a JSON string is unmarshalled to different AggsResponse
	// implementations, which may be used on the client side.
	// see: internal/common/consts/aggregation.go.
	Type    string      `json:"_type"`
	Value   interface{} `json:"value,omitempty"`
	Buckets []Bucket    `json:"buckets,omitempty"`
}

type Bucket map[string]interface{}
