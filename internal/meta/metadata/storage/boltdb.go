package storage

import (
	"bytes"
	"errors"
	"time"

	"github.com/boltdb/bolt"
)

type boltMetaStore struct {
	db *bolt.DB
}

func Open() (*boltMetaStore, error) {
	// Open the data file in your current directory.
	// It will be created if it doesn't exist.
	if db, err := bolt.Open("_meta.bolt", 0600, &bolt.Options{Timeout: 1 * time.Second}); err != nil {
		return nil, err
	} else {
		return &boltMetaStore{db}, nil
	}
}

func Close(store *boltMetaStore) error {
	return store.db.Close()
}

func (store *boltMetaStore) Get(path string) ([]byte, error) {
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

func (store *boltMetaStore) Set(path string, val []byte) error {
	bkt, key := splitPath(path)
	return store.db.Update(func(tx *bolt.Tx) error {
		if bucket, err := tx.CreateBucketIfNotExists(bkt); err != nil {
			return err
		} else {
			return bucket.Put(key, val)
		}
	})
}

func (store *boltMetaStore) Delete(path string) error {
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
