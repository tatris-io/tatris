// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

type IngestRequest struct {
	Index     string                   `json:"index"`
	Documents []map[string]interface{} `json:"documents"`
}
