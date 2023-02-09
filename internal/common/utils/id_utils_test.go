// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package utils

import (
	"sync"
	"testing"

	"gotest.tools/v3/assert"
)

type TestCase struct {
	sw             *sync.WaitGroup
	loop           int
	goroutineCount int
}

func TestGenerateID(t *testing.T) {
	cond := TestCase{sw: new(sync.WaitGroup), loop: 1000, goroutineCount: 200}
	cond.sw.Add(cond.goroutineCount)
	var ids sync.Map
	for i := 0; i < cond.goroutineCount; i++ {
		go func() {
			defer cond.sw.Done()
			for i := 0; i < cond.loop; i++ {
				if id, err := GenerateID(); err == nil {
					ids.Store(id, id)
				}
			}
		}()
	}
	cond.sw.Wait()
	realLen := 0
	ids.Range(func(key, value any) bool {
		realLen++
		return true
	})
	expectLen := cond.goroutineCount * cond.loop
	t.Logf("expectLen: %d realLen : %d", expectLen, realLen)
	assert.Equal(t, realLen, expectLen)
}
