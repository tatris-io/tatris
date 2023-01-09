// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package indexlib organizes codes of the indexing library
package indexlib

const (
	BlugeIndexLibType = "bluge"
	DefaultDataPath   = "./_data"
	FSStorageType     = "fs"
)

type BaseConfig struct {
	StorageType  string
	DataPath     string
	Index        string
	IndexLibType string
}

func NewBaseConfig(config *BaseConfig) *BaseConfig {
	if config.DataPath == "" {
		config.DataPath = DefaultDataPath
	}
	if config.StorageType == "" {
		config.StorageType = FSStorageType
	}
	if config.IndexLibType == "" {
		config.IndexLibType = BlugeIndexLibType
	}
	return config
}
