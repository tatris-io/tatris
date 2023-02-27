// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

type IngestResponse struct {
	Took    int64  `json:"took"`
	Error   bool   `json:"error"`
	Message string `json:"message"`
}
