// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package log defines the abstract public trait of WAL behavior
package log

type WalLog interface {
	Write(data []byte) error
	BWrite(datas [][]byte) error
	Read(index uint64) ([]byte, error)
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	TruncateFront(id uint64) error
	TruncateBack(id uint64) error
	Close() error
}
