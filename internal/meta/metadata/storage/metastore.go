// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package storage is about the physical storage of metadata
package storage

type MetaStore interface {
	Set(string, []byte) error
	Get(string) ([]byte, error)
	Delete(string) error
	Close() error
}
