// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package boltdb describes an implementation of boltdb-based metadata storage
package boltdb

import (
	"bytes"
	"os"
	"path"

	"github.com/tatris-io/tatris/internal/common/consts"

	"github.com/tatris-io/tatris/internal/core/config"

	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/common/utils"

	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/meta/metadata/storage"
	"go.etcd.io/bbolt"
)

type BoltMetaStore struct {
	db *bbolt.DB
}

const SuffixBolt = ".bolt"

func Open() (storage.MetaStore, error) {
	p := path.Join(config.Cfg.GetFSPath(), consts.PathMeta) + SuffixBolt
	logger.Info("open boltdb", zap.String("path", p))
	d := path.Dir(p)
	// mkdir
	err := os.MkdirAll(d, 0755)
	if err != nil {
		return nil, err
	}
	// Open the data file.
	// It will be created if it doesn't exist.
	var db *bbolt.DB
	db, err = bbolt.Open(p, 0644, nil)
	if err != nil {
		return nil, err
	}
	return &BoltMetaStore{db}, nil
}

func (store *BoltMetaStore) Close() error {
	return store.db.Close()
}

func (store *BoltMetaStore) Get(path string) ([]byte, error) {
	defer utils.Timerf("boltdb get finish, path:%s", path)()
	var result []byte
	bkt, key := splitPath(path)
	err := store.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bkt)
		if bucket == nil {
			return nil
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
	defer utils.Timerf("boltdb set finish, path:%s", path)()
	bkt, key := splitPath(path)
	return store.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bkt)
		if err != nil {
			return err
		}
		return bucket.Put(key, val)
	})
}

func (store *BoltMetaStore) List(prefix string) (map[string][]byte, error) {
	defer utils.Timerf("boltdb list finish, prefix:%s", prefix)()
	bkt, _ := splitPath(prefix)
	results := make(map[string][]byte)
	err := store.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bkt)
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		for key, val := cursor.First(); key != nil; key, val = cursor.Next() {
			copiedVal := make([]byte, len(val))
			copy(copiedVal, val)
			results[string(key)] = copiedVal
		}
		return nil
	})
	return results, err
}

func (store *BoltMetaStore) Delete(path string) error {
	defer utils.Timerf("boltdb delete finish, path:%s", path)()
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
