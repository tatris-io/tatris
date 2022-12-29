// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package bluge

import (
	"github.com/blugelabs/bluge"
	"github.com/tatris-io/tatris/internal/indexlib/bluge/config"
)

func GetWriter(storageType string, dataPath string, indexName string) (*bluge.Writer, error) {
	var cfg bluge.Config

	switch storageType {
	case "fs":
		cfg = config.GetFSConfig(dataPath, indexName)
	default:
		cfg = config.GetFSConfig(dataPath, indexName)
	}

	writer, err := bluge.OpenWriter(cfg)
	if err != nil {
		return nil, err
	}

	return writer, nil
}

func CloseWriter(writer *bluge.Writer) {
	writer.Close()
}
