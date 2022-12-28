// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package storage

type MetaStore interface {
	Set(string, []byte) error
	Get(string) ([]byte, error)
	Delete(string) error
}
