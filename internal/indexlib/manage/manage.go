// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package manage organizes codes of the indexing library manage
package manage

import (
	"errors"
	"github.com/tatris-io/tatris/internal/indexlib"
	"github.com/tatris-io/tatris/internal/indexlib/bluge"
	"log"
)

// GetReader The Reader represents a stable snapshot of the index a point in time.
// This means that changes made to the index after the reader is obtained never affect the results returned by this reader.
// This also means that this Reader is holding onto resources and MUST be closed when it is no longer needed.
func GetReader(config *indexlib.BaseConfig) (indexlib.Reader, error) {
	if config.Index == "" {
		return nil, errors.New("no index specified")
	}

	baseConfig := indexlib.SetDefaultConfig(config)

	switch baseConfig.IndexLibType {
	case indexlib.BlugeIndexLibType:
		blugeReader := bluge.NewBlugeReader(baseConfig)
		err := blugeReader.OpenReader()
		if err != nil {
			log.Printf("bluge open reader error: %s", err)
			return nil, err
		}
		return blugeReader, nil
	default:
		return nil, errors.New("index lib not support")
	}
}

// GetWriter Writer’s hold an exclusive-lock on their underlying directory which prevents other processes from opening a writer while this one is still open.
// This does not affect Readers that are already open, and it does not prevent new Readers from being opened,
// but it does mean care care should be taken to close the Writer when you done.
func GetWriter(config *indexlib.BaseConfig) (indexlib.Writer, error) {
	if config.Index == "" {
		return nil, errors.New("no index specified")
	}

	baseConfig := indexlib.SetDefaultConfig(config)

	switch baseConfig.IndexLibType {
	case indexlib.BlugeIndexLibType:
		blugeWriter := bluge.NewBlugeWriter(baseConfig)
		err := blugeWriter.OpenWriter()
		if err != nil {
			log.Printf("bluge open writer error: %s", err)
			return nil, err
		}
		return blugeWriter, nil
	default:
		return nil, errors.New("index lib not support")
	}
}
