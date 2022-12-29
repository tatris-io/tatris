// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package bluge organizes codes of the indexing library bluge
package bluge

import (
	"github.com/blugelabs/bluge"
	"github.com/tatris-io/tatris/internal/indexlib/bluge/config"
)

func GetReader(storageType string, dataPath string, indexName string) (*bluge.Reader, error) {
	var cfg bluge.Config

	switch storageType {
	case "fs":
		cfg = config.GetFSConfig(dataPath, indexName)
	default:
		cfg = config.GetFSConfig(dataPath, indexName)
	}

	reader, err := bluge.OpenReader(cfg)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

func CloseReader(reader *bluge.Reader) {
	reader.Close()
}
