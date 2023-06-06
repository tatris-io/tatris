// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package core

import (
	"github.com/pkg/errors"
	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/errs"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/bluge"
	"go.uber.org/zap"
)

// mergeReader merges opened readers into one indexlib.Reader instance. Now the provided reader must
// be type of *bluge.BlugeReader.
func mergeReader(
	config *indexlib.Config,
	segments []string,
	readers []indexlib.Reader,
) (indexlib.Reader, error) {
	switch config.IndexLib {
	case consts.IndexLibBluge:
		blugeReader, err := bluge.MergeReader(config, segments, readers)
		if err != nil {
			logger.Error("bluge fail to merge readers", zap.Error(err))
			return nil, err
		}
		err = blugeReader.OpenReader()
		if err != nil {
			logger.Error("bluge open reader failed", zap.Error(err))
			return nil, err
		}
		return blugeReader, nil
	default:
		return nil, errs.ErrIndexLibNotSupport
	}
}

// MergeSegmentReader merges segment readers into one indexlib.Reader instance. Now the provided
// reader must be type of *bluge.BlugeReader.
func MergeSegmentReader(
	config *indexlib.Config,
	segments ...*Segment,
) (indexlib.Reader, error) {
	segNames := make([]string, 0, len(segments))
	readers := make([]indexlib.Reader, 0, len(segments))
	var lastGetReaderErr error
	for _, segment := range segments {
		if reader, err := segment.GetReader(); err == nil {
			segNames = append(segNames, segment.GetName())
			readers = append(readers, reader)
		} else {
			lastGetReaderErr = err
			logger.Error("fail to open segment reader", zap.String("segment", segment.GetName()), zap.Error(err))
		}
	}
	if len(readers) == 0 {
		return nil, errors.Wrap(lastGetReaderErr, "fail to open segment reader")
	}

	merged, err := mergeReader(config, segNames, readers)
	if err != nil {
		for _, reader := range readers {
			reader.Close()
		}
	}
	return merged, err
}
