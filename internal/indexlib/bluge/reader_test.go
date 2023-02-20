// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.
package bluge

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCloseHook(t *testing.T) {
	closed := false
	b := &BlugeReader{
		closeHook: func(reader *BlugeReader) {
			closed = true
		},
	}
	b.Close()
	assert.True(t, closed)
}
