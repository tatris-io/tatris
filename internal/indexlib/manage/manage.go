// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package manage organizes codes of the indexing library manage
package manage

import (
	"errors"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/bluge"
	"log"
	"strings"
	"sync"
)

var readerPool sync.Map
var writerPool sync.Map

func GetReader(config *indexlib.BaseConfig) (indexlib.Reader, error) {
	if config.Index == "" {
		return nil, errors.New("no index specified")
	}

	baseConfig := indexlib.NewBaseConfig(config)
	key := getKey(baseConfig)
	if reader, found := readerPool.Load(key); found {
		return reader.(indexlib.Reader), nil
	}
	// First get Near-Real-Time reader
	if writer, found := writerPool.Load(key); found {
		reader, err := writer.(indexlib.Writer).Reader()
		if err != nil {
			return nil, err
		}
		readerPool.Store(key, reader)
		return reader, nil
	}

	switch baseConfig.IndexLibType {
	case indexlib.BlugeIndexLibType:
		blugeReader := bluge.NewBlugeReader(baseConfig)
		err := blugeReader.OpenReader()
		if err != nil {
			log.Printf("bluge open reader error: %s", err)
			return nil, err
		}
		readerPool.Store(key, blugeReader)
		return blugeReader, nil
	default:
		return nil, errors.New("index lib not support")
	}
}

func GetWriter(config *indexlib.BaseConfig) (indexlib.Writer, error) {
	if config.Index == "" {
		return nil, errors.New("no index specified")
	}

	baseConfig := indexlib.NewBaseConfig(config)
	key := getKey(baseConfig)
	if writer, found := writerPool.Load(key); found {
		return writer.(indexlib.Writer), nil
	}

	switch baseConfig.IndexLibType {
	case indexlib.BlugeIndexLibType:
		blugeWriter := bluge.NewBlugeWriter(baseConfig)
		err := blugeWriter.OpenWriter()
		if err != nil {
			log.Printf("bluge open writer error: %s", err)
			return nil, err
		}
		writerPool.Store(key, blugeWriter)
		return blugeWriter, nil
	default:
		return nil, errors.New("index lib not support")
	}
}

func CloseReader(config *indexlib.BaseConfig) {
	baseConfig := indexlib.NewBaseConfig(config)
	key := getKey(baseConfig)
	if reader, found := readerPool.Load(key); found {
		reader.(indexlib.Reader).Close()
		readerPool.Delete(key)
	}
}

func CloseWriter(config *indexlib.BaseConfig) {
	baseConfig := indexlib.NewBaseConfig(config)
	key := getKey(baseConfig)
	if writer, found := writerPool.Load(key); found {
		writer.(indexlib.Writer).Close()
		writerPool.Delete(key)
	}
}

func getKey(config *indexlib.BaseConfig) string {
	return strings.Join([]string{config.IndexLibType, config.StorageType, config.Index}, "-")
}
