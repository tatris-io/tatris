// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package protocol

type BulkAction map[string]*BulkMeta

type BulkMeta struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
}
