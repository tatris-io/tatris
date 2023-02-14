// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package manage organizes codes of the indexing library manage
package manage

import (
	"github.com/tatris-io/tatris/internal/protocol"

	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/bluge"
	"go.uber.org/zap"
)

// GetReader The Reader represents a stable snapshot of the index a point in time.
// This means that changes made to the index after the reader is obtained never affect the results
// returned by this reader. This also means that this Reader is holding onto resources and MUST be
// closed when it is no longer needed.
func GetReader(
	config *indexlib.BaseConfig,
	mappings *protocol.Mappings,
	index ...string,
) (indexlib.Reader, error) {
	indexlib.SetDefaultConfig(config)
	switch config.IndexLibType {
	case indexlib.BlugeIndexLibType:
		blugeReader := bluge.NewBlugeReader(config, mappings, index...)
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

// GetWriter Writer’s hold an exclusive-lock on their underlying directory which prevents other
// processes from opening a writer while this one is still open. This does not affect Readers that
// are already open, and it does not prevent new Readers from being opened,
// but it does mean care should be taken to close the Writer when your work done.
func GetWriter(
	config *indexlib.BaseConfig,
	mappings *protocol.Mappings,
	index string,
) (indexlib.Writer, error) {
	baseConfig := indexlib.SetDefaultConfig(config)

	switch baseConfig.IndexLibType {
	case indexlib.BlugeIndexLibType:
		blugeWriter := bluge.NewBlugeWriter(baseConfig, mappings, index)
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
