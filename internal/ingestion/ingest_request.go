// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package ingestion

type IngestRequest struct {
	Documents []map[string]interface{} `json:"documents"`
}
