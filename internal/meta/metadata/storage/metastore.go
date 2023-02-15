// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package storage is about the physical storage of metadata
package storage

type MetaStore interface {
	// Set Set("/x/y/z", bytes)
	Set(string, []byte) error
	// Get Get("/x/y/z")
	Get(string) ([]byte, error)
	// List List("/x/y/")
	List(string) (map[string][]byte, error)
	// Delete Delete("/x/y/z")
	Delete(string) error
	Close() error
}
