// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

import "time"

type QueryResponse struct {
	Took int64 `json:"took"`
	Hits Hits  `json:"hits"`
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
	Index     string                 `json:"_index"`
	ID        string                 `json:"_id"`
	Source    map[string]interface{} `json:"_source"`
	Timestamp time.Time              `json:"@timestamp"`
}
