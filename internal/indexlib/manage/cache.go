// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package manage

import (
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/indexlib"
)

const (
	// defaultCloseDelay
	defaultCloseDelay = time.Minute
	// defaultExpireTime
	defaultExpireTime = 10 * time.Minute
	// defaultExpireCheckInterval is the default check interval
	defaultExpireCheckInterval = 1 * time.Minute
)

type (
	// readerCache caches opened index reader whose construction has a significant cost.
	// When a reader becomes idle, it will be closed automatically.
	readerCache struct {
		// cache stores segment name to its reader
		// segment name format: ${index}/${shardId}/${segmentId}. for example 'foo/0/0'
		cache      *cache.Cache
		mutex      sync.RWMutex
		closeDelay time.Duration
	}
)

func newReaderCache(defaultExpiration, cleanupInterval, closeDelay time.Duration) *readerCache {
	rc := &readerCache{
		closeDelay: closeDelay,
	}
	c := cache.New(defaultExpiration, cleanupInterval)
	c.OnEvicted(rc.onItemEvicted)
	rc.cache = c
	return rc
}

func (c *readerCache) PutIfAbsent(key string, reader indexlib.Reader) (indexlib.Reader, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if cached, ok := c.cache.Get(key); ok {
		return cached.(*indexlib.HookReader), false
	}

	entry := &indexlib.HookReader{
		Reader: reader,
		CloseHook: func(reader indexlib.Reader) {
			// no close
		},
	}

	c.cache.SetDefault(key, entry)
	return entry, true
}

// Get returns reader with specified key
func (c *readerCache) Get(key string) (indexlib.Reader, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if cached, ok := c.cache.Get(key); ok {
		return cached.(*indexlib.HookReader), true
	}

	return nil, false
}

func (c *readerCache) onItemEvicted(key string, i interface{}) {
	reader := i.(*indexlib.HookReader)
	time.AfterFunc(c.closeDelay, func() {
		reader.Reader.Close()
		logger.Infof("close cached reader %s", key)
	})
}
