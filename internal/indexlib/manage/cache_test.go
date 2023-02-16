// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package manage

import (
	"fmt"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/assert"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/bluge"
)

func TestReaderCache(t *testing.T) {
	cache := newReaderCache(time.Second, 200*time.Millisecond, time.Second)

	reader0 := &bluge.BlugeReader{}
	reader1, put := cache.PutIfAbsent("foo", reader0)
	assert.True(t, put)
	assert.NotNil(t, reader1)
	assert.IsType(t, &indexlib.HookReader{}, reader1)
	assert.Same(t, indexlib.UnwrapReader(reader1), reader0)

	reader2, put := cache.PutIfAbsent("foo", &bluge.BlugeReader{})
	assert.False(t, put)
	assert.NotNil(t, reader2)
	assert.Same(t, reader1, reader2)

	reader3, ok := cache.Get("foo")
	assert.True(t, ok)
	assert.NotNil(t, reader3)
	assert.Same(t, reader1, reader3)

	// 2 > 0.2(check) + 1(expire)
	time.Sleep(2 * time.Second)

	// expired
	_, ok = cache.Get("foo")
	assert.False(t, ok)
}

func TestCache(_ *testing.T) {
	c := cache.New(time.Second, 200*time.Millisecond)
	c.OnEvicted(func(key string, i interface{}) {
		fmt.Println("evicted", key, i)
	})
	c.SetDefault("a", 1)
	time.Sleep(500 * time.Millisecond)
	c.SetDefault("a", 2)
	time.Sleep(1 * time.Second)
}
