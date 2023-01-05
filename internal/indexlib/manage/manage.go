// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package manage organizes codes of the indexing library manage
package manage

import (
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/bluge"
	"log"
	"strings"
)

var readerPool map[string]indexlib.Reader
var writerPool map[string]indexlib.Writer

func init() {
	readerPool = make(map[string]indexlib.Reader)
	writerPool = make(map[string]indexlib.Writer)
}

func GetReader(config *indexlib.BaseConfig) indexlib.Reader {
	if config.Index == "" {
		return nil
	}

	baseConfig := indexlib.NewBaseConfig(config)
	key := getKey(baseConfig)
	if reader, found := readerPool[key]; found {
		return reader
	}

	switch baseConfig.IndexLibType {
	case indexlib.BlugeIndexLibType:
		blugeReader := bluge.NewBlugeReader(baseConfig)
		err := blugeReader.OpenReader()
		if err != nil {
			log.Printf("bluge open reader error: %s", err)
			return nil
		}
		readerPool[key] = blugeReader
		return blugeReader
	default:
		log.Printf("index lib not support")
	}

	return nil
}

func GetWriter(config *indexlib.BaseConfig) indexlib.Writer {
	if config.Index == "" {
		return nil
	}

	baseConfig := indexlib.NewBaseConfig(config)
	key := getKey(baseConfig)
	if writer, found := writerPool[key]; found {
		return writer
	}

	switch baseConfig.IndexLibType {
	case indexlib.BlugeIndexLibType:
		blugeWriter := bluge.NewBlugeWriter(baseConfig)
		err := blugeWriter.OpenWriter()
		if err != nil {
			log.Printf("bluge open writer error: %s", err)
			return nil
		}
		writerPool[key] = blugeWriter
		return blugeWriter
	default:
		log.Printf("index lib not support")
	}

	return nil
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
