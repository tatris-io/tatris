// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package boltdb describes an implementation of boltdb-based metadata storage
package boltdb

import (
	"bytes"
	"errors"
	"github.com/tatris-io/tatris/internal/meta/metadata/storage"
	"go.etcd.io/bbolt"
	"time"
)

const (
	BoltMetaPath = "/tmp/tatris/_meta.bolt"
)

type BoltMetaStore struct {
	db *bbolt.DB
}

func Open() (storage.MetaStore, error) {
	// Open the data file.
	// It will be created if it doesn't exist.
	db, err := bbolt.Open(BoltMetaPath, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	return &BoltMetaStore{db}, nil
}

func (store *BoltMetaStore) Close() error {
	return store.db.Close()
}

func (store *BoltMetaStore) Get(path string) ([]byte, error) {
	var result []byte
	bkt, key := splitPath(path)
	err := store.db.View(func(tx *bbolt.Tx) error {
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
	return store.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bkt)
		if err != nil {
			return err
		}
		return bucket.Put(key, val)
	})
}

func (store *BoltMetaStore) Delete(path string) error {
	bkt, key := splitPath(path)
	return store.db.Update(func(tx *bbolt.Tx) error {
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
