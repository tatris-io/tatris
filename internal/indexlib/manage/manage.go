// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package manage organizes codes of the indexing library manage
package manage

import (
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/bluge"
	"github.com/tatris-io/tatris/internal/protocol"
	"go.uber.org/zap"
)

var (
	defaultReaderCache = newReaderCache(
		defaultExpireTime,
		defaultExpireCheckInterval,
		defaultCloseDelay,
	)
)

// GetReader The Reader represents a stable snapshot of the index a point in time.
// This means that changes made to the index after the reader is obtained never affect the results
// returned by this reader. This also means that this Reader is holding onto resources and MUST be
// closed when it is no longer needed.
func GetReader(
	config *indexlib.BaseConfig,
	segments ...string,
) (indexlib.Reader, error) {
	// TODO: when we support update mappings when an index running, segments(represented by 'index'
	// var here) of the index may have different mappings
	indexlib.SetDefaultConfig(config)
	switch config.IndexLibType {
	case indexlib.BlugeIndexLibType:
		blugeReader := bluge.NewBlugeReader(config, segments...)
		err := blugeReader.OpenReader()
		if err != nil {
			logger.Error("bluge open reader failed", zap.Error(err))
			return nil, err
		}
		return blugeReader, nil
	default:
		return nil, errs.ErrIndexLibNotSupport
	}
}

// GetReaderUsingCache tries to get reader from cache firstly, or open a new reader and cache it.
func GetReaderUsingCache(config *indexlib.BaseConfig, index string) (indexlib.Reader, error) {
	// TODO: Here we didn't lock across 'defaultReaderCache.Get()' and
	// 'defaultReaderCache.PutIfAbsent()'
	// Because 'GetReader' is a heavy operation, which takes long time.
	// Attention: When this func is called with same 'index' arg many times in a short time, it
	// causes 'Cache Breakdown'.
	// If this does matter, extra optimizations are required.

	if reader, ok := defaultReaderCache.Get(index); ok {
		return reader, nil
	}

	reader, err := GetReader(config, index)
	if err != nil {
		return nil, err
	}

	finalReader, put := defaultReaderCache.PutIfAbsent(index, reader)
	if !put {
		// There is already a reader(ref by finalReader) in the cache. Close the redundant one.
		reader.Close()
	}
	return finalReader, nil
}

// GetWriter Writer’s hold an exclusive-lock on their underlying directory which prevents other
// processes from opening a writer while this one is still open. This does not affect Readers that
// are already open, and it does not prevent new Readers from being opened,
// but it does mean care should be taken to close the Writer when your work done.
func GetWriter(
	config *indexlib.BaseConfig,
	mappings *protocol.Mappings,
	index string,
	segment string,
) (indexlib.Writer, error) {
	baseConfig := indexlib.SetDefaultConfig(config)

	switch baseConfig.IndexLibType {
	case indexlib.BlugeIndexLibType:
		blugeWriter := bluge.NewBlugeWriter(baseConfig, mappings, index, segment)
		err := blugeWriter.OpenWriter()
		if err != nil {
			logger.Error("bluge open writer failed", zap.Error(err))
			return nil, err
		}
		return blugeWriter, nil
	default:
		return nil, errs.ErrIndexLibNotSupport
	}
}
