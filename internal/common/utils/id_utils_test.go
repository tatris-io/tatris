package utils

import (
	"gotest.tools/v3/assert"
	"sync"
	"testing"
)

type TestCase struct {
	sw             *sync.WaitGroup
	loop           int
	goroutineCount int
}

func TestGenerateID(t *testing.T) {
	cond := TestCase{sw: new(sync.WaitGroup), loop: 10000, goroutineCount: 200}
	cond.sw.Add(cond.goroutineCount)
	var ids sync.Map
	for i := 0; i < cond.goroutineCount; i++ {
		go func() {
			for i := 0; i < cond.loop; i++ {
				if id, err := GenerateID(); err == nil {
					ids.Store(id, id)
				}
			}
			defer cond.sw.Done()
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
