// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package manage

import (
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"go.uber.org/zap"

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
	logger.Debug("[readerCache] put", zap.String("key", key))
	return entry, true
}

// Get returns reader with specified key
func (c *readerCache) Get(key string) (indexlib.Reader, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if cached, ok := c.cache.Get(key); ok {
		logger.Debug("[readerCache] hit", zap.String("key", key))
		return cached.(*indexlib.HookReader), true
	}

	return nil, false
}

func (c *readerCache) onItemEvicted(key string, i interface{}) {
	logger.Debug("[readerCache] onItemEvicted", zap.String("key", key))
	reader := i.(*indexlib.HookReader)

	time.AfterFunc(c.closeDelay, func() {
		logger.Debug("[readerCache] close reader", zap.String("key", key))
		reader.Reader.Close()
	})
}
