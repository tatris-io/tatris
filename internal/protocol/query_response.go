// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

type QueryResponse struct {
	Took     int64       `json:"took"`
	TimedOut bool        `json:"timedOut"`
	Shards   Shards      `json:"_shards"`
	Hits     Hits        `json:"hits"`
	Error    interface{} `json:"error"`
	Status   int32       `json:"status"`
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
	Index  string                 `json:"_index"`
	ID     string                 `json:"_id"`
	Source map[string]interface{} `json:"_source"`
}
