// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package indexlib

type Writer interface {
	OpenWriter() error
	Insert(docID string, doc map[string]interface{}) error
	Batch(docs map[string]map[string]interface{}) error
	Reader() (Reader, error)
	Close()
}
