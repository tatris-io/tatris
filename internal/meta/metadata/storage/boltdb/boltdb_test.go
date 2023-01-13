// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package boltdb

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBoltMetaStore_Get(t *testing.T) {
	boltMetaStore, _ := Open()
	path := "/tmp/tatris/kk"
	val := "vv"
	t.Run("set", func(t *testing.T) {
		err := boltMetaStore.Set(path, []byte("vv"))
		assert.NoError(t, err)
	})
	t.Run("get", func(t *testing.T) {
		result, err := boltMetaStore.Get(path)
		println(string(result))
		assert.NoError(t, err)
		assert.Equal(t, string(result), val)
	})
	t.Run("delete", func(t *testing.T) {
		err := boltMetaStore.Delete(path)
		assert.NoError(t, err)
	})

}
