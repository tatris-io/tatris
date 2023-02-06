// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package tidwall encapsulates the WAL solution implemented by tidwall/wal
package tidwall

import (
	"sync"

	"github.com/tidwall/wal"
)

type TWalLog struct {
	Log  *wal.Log
	Lock sync.Mutex
}

func (twal *TWalLog) Write(data []byte) error {
	twal.Lock.Lock()
	defer twal.Lock.Unlock()
	lastIndex, err := twal.Log.LastIndex()
	if err != nil {
		return err
	}
	return twal.Log.Write(lastIndex+1, data)
}

func (twal *TWalLog) BWrite(datas [][]byte) error {
	twal.Lock.Lock()
	defer twal.Lock.Unlock()
	lastIndex, err := twal.Log.LastIndex()
	if err != nil {
		return err
	}
	b := new(wal.Batch)
	for i, data := range datas {
		b.Write(lastIndex+uint64(i+1), data)
	}
	return twal.Log.WriteBatch(b)
}

func (twal *TWalLog) Read(index uint64) ([]byte, error) {
	return twal.Log.Read(index)
}

func (twal *TWalLog) FirstIndex() (index uint64, err error) {
	return twal.Log.FirstIndex()
}

func (twal *TWalLog) LastIndex() (index uint64, err error) {
	return twal.Log.LastIndex()
}

func (twal *TWalLog) TruncateFront(id uint64) error {
	return twal.Log.TruncateFront(id)
}

func (twal *TWalLog) TruncateBack(id uint64) error {
	return twal.Log.TruncateBack(id)
}

func (twal *TWalLog) Close() error {
	return twal.Log.Close()
}
