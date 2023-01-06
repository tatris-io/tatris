// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package storage is about how to implement persistent storage of metadata
package storage

import (
	"bytes"
	"errors"
	"time"

	"github.com/boltdb/bolt"
)

const (
	BoltMetaPath = "/tmp/tatris/_meta.bolt"
)

type BoltMetaStore struct {
	db *bolt.DB
}

func Open() (*BoltMetaStore, error) {
	// Open the data file in your current directory.
	// It will be created if it doesn't exist.
	db, err := bolt.Open(BoltMetaPath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	return &BoltMetaStore{db}, nil
}

func Close(store *BoltMetaStore) error {
	return store.db.Close()
}

func (store *BoltMetaStore) Get(path string) ([]byte, error) {
	var result []byte
	bkt, key := splitPath(path)
	err := store.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bkt)
		if bucket == nil {
			return errors.New("bucket not found: " + string(bkt))
		}
		val := bucket.Get(key)
		if val != nil {
			result = make([]byte, len(val))
			copy(result, val)
		}
		return nil
	})
	return result, err
}

func (store *BoltMetaStore) Set(path string, val []byte) error {
	bkt, key := splitPath(path)
	return store.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bkt)
		if err != nil {
			return err
		}
		return bucket.Put(key, val)
	})
}

func (store *BoltMetaStore) Delete(path string) error {
	bkt, key := splitPath(path)
	return store.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bkt)
		if bucket != nil {
			return bucket.Delete(key)
		}
		return nil
	})
}

func splitPath(path string) ([]byte, []byte) {
	pb := []byte(path)
	i := bytes.LastIndex(pb, []byte("/"))
	return pb[:i], pb[i+1:]
}
