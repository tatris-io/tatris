// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package storage is about the physical storage of metadata
package storage

type MetaStore interface {
	// Set sets the value for a key, bucket will be created if it does not exist
	// example: Set("/x/y/z", bytes)
	Set(string, []byte) error
	// Get retrieves the value for a key
	// example: Get("/x/y/z")
	Get(string) ([]byte, error)
	// List returns all values under the bucket
	// example: List("/x/y/")
	List(string) (map[string][]byte, error)
	// Delete removes a key from the bucket
	// example: Delete("/x/y/z")
	Delete(string) error
	Close() error
}
