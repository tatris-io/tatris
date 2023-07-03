// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package utils

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetLocalIP(t *testing.T) {
	ip, err := GetLocalIP()
	t.Log(ip)
	assert.Equal(t, err, nil)
}
