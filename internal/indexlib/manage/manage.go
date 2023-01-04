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
	key := strings.Join([]string{baseConfig.IndexLibType, baseConfig.StorageType, baseConfig.Index}, "-")
	if reader, found := readerPool[key]; found {
		return reader
	} else {
		switch baseConfig.IndexLibType {
		case indexlib.BlugeIndexLibType:
			blugeReader := bluge.NewBlugeReader(baseConfig)
			blugeReader.OpenReader()
			readerPool[key] = blugeReader
			return blugeReader
		default:
			log.Printf("index lib not support")
		}
	}

	return nil
}

func GetWriter(config *indexlib.BaseConfig) indexlib.Writer {
	if config.Index == "" {
		return nil
	}

	baseConfig := indexlib.NewBaseConfig(config)
	key := strings.Join([]string{baseConfig.IndexLibType, baseConfig.StorageType, baseConfig.Index}, "-")
	if writer, found := writerPool[key]; found {
		return writer
	} else {
		switch baseConfig.IndexLibType {
		case indexlib.BlugeIndexLibType:
			blugeWriter := bluge.NewBlugeWriter(baseConfig)
			blugeWriter.OpenWriter()
			writerPool[key] = blugeWriter
			return blugeWriter
		default:
			log.Printf("index lib not support")
		}
	}

	return nil
}

func CloseReader(config *indexlib.BaseConfig) {
	baseConfig := indexlib.NewBaseConfig(config)
	key := strings.Join([]string{baseConfig.IndexLibType, baseConfig.StorageType, baseConfig.Index}, "-")
	if reader, found := readerPool[key]; found {
		reader.Close()
		delete(readerPool, key)
	}
}

func CloseWriter(config *indexlib.BaseConfig) {
	baseConfig := indexlib.NewBaseConfig(config)
	key := strings.Join([]string{baseConfig.IndexLibType, baseConfig.StorageType, baseConfig.Index}, "-")
	if writer, found := writerPool[key]; found {
		writer.Close()
		delete(writerPool, key)
	}
}
