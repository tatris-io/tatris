// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

import "github.com/tatris-io/tatris/internal/protocol"

type Writer interface {
	OpenWriter() error
	Insert(docID string, doc protocol.Document) error
	Batch(docs map[string]protocol.Document) error
	Reader() (Reader, error)
	Close()
}
