// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

import "github.com/tatris-io/tatris/internal/protocol"

type Writer interface {
	OpenWriter() error
	Insert(docID string, doc map[string]interface{}, mappings *protocol.Mappings) error
	Batch(docs map[string]map[string]interface{}, mappings *protocol.Mappings) error
	Reader() (Reader, error)
	Close()
}
