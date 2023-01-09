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

var readerPool map[string]indexlib.Reader
var writerPool map[string]indexlib.Writer

var readerLock sync.Mutex
var writerLock sync.Mutex

func init() {
	readerPool = make(map[string]indexlib.Reader)
	writerPool = make(map[string]indexlib.Writer)
}

func GetReader(config *indexlib.BaseConfig) (indexlib.Reader, error) {
	if config.Index == "" {
		return nil, errors.New("no index specified")
	}

	readerLock.Lock()
	defer readerLock.Unlock()

	baseConfig := indexlib.NewBaseConfig(config)
	key := getKey(baseConfig)
	if reader, found := readerPool[key]; found {
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
		readerPool[key] = blugeReader
		return blugeReader, nil
	default:
		return nil, errors.New("index lib not support")
	}
}

func GetWriter(config *indexlib.BaseConfig) (indexlib.Writer, error) {
	if config.Index == "" {
		return nil, errors.New("no index specified")
	}

	writerLock.Lock()
	defer writerLock.Unlock()

	baseConfig := indexlib.NewBaseConfig(config)
	key := getKey(baseConfig)
	if writer, found := writerPool[key]; found {
		return writer, nil
	}

	switch baseConfig.IndexLibType {
	case indexlib.BlugeIndexLibType:
		blugeWriter := bluge.NewBlugeWriter(baseConfig)
		err := blugeWriter.OpenWriter()
		if err != nil {
			log.Printf("bluge open writer error: %s", err)
			return nil, err
		}
		writerPool[key] = blugeWriter
		return blugeWriter, nil
	default:
		return nil, errors.New("index lib not support")
	}
}

func CloseReader(config *indexlib.BaseConfig) {
	baseConfig := indexlib.NewBaseConfig(config)
	key := getKey(baseConfig)
	if reader, found := readerPool[key]; found {
		reader.Close()
		delete(readerPool, key)
	}
}

func CloseWriter(config *indexlib.BaseConfig) {
	baseConfig := indexlib.NewBaseConfig(config)
	key := getKey(baseConfig)
	if writer, found := writerPool[key]; found {
		writer.Close()
		delete(writerPool, key)
	}
}

func getKey(config *indexlib.BaseConfig) string {
	return strings.Join([]string{config.IndexLibType, config.StorageType, config.Index}, "-")
}
