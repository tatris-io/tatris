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
	cond := TestCase{sw: new(sync.WaitGroup), loop: 5000, goroutineCount: 200}
	cond.sw.Add(cond.goroutineCount)
	ids := make(map[string]string)
	mutex := sync.Mutex{}
	for i := 0; i < cond.goroutineCount; i++ {
		go func() {
			defer cond.sw.Done()
			defer mutex.Unlock()
			mutex.Lock()
			for i := 0; i < cond.loop; i++ {
				if docID, err := GenerateID(); err == nil {
					ids[docID] = docID
				}
			}
		}()
	}
	cond.sw.Wait()
	realLen := len(ids)
	expectLen := cond.goroutineCount * cond.loop
	t.Logf("expectLen: %d realLen : %d", expectLen, realLen)
	assert.Equal(t, len(ids), expectLen)
}
