// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package config organizes codes of the bluge config
package config

import (
	"path"

	"github.com/blugelabs/bluge"
)

func GetFSConfig(datePath string, indexName string) bluge.Config {
	return bluge.DefaultConfig(path.Join(datePath, indexName))
}
