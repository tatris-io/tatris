package storage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBoltMetaStore_Get(t *testing.T) {
	boltMetaStore, _ := Open()
	defer Close(boltMetaStore)
	path := "/tatris/kk"
	val := "vv"
	t.Run("prepare", func(t *testing.T) {
		err := boltMetaStore.Set(path, []byte("vv"))
		assert.NoError(t, err)
	})
	t.Run("get", func(t *testing.T) {
		result, err := boltMetaStore.Get(path)
		println(string(result))
		assert.NoError(t, err)
		assert.Equal(t, string(result), val)
	})

}
