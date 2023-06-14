// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package indexlib organizes codes of the indexing library
package indexlib

import (
	"path"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/core/config"
)

type Config struct {
	IndexLib      string
	DirectoryType string
	FS            *FileSystem
	OSS           *ObjectStorageService
}

type FileSystem struct {
	Path string
}

type ObjectStorageService struct {
	Endpoint                   string
	Bucket                     string
	AccessKeyID                string
	SecretAccessKey            string
	CacheDir                   string
	MinimumConcurrencyLoadSize int
}

func BuildConf(directory *config.Directory) *Config {
	cfg := &Config{
		IndexLib:      consts.IndexLibBluge,
		DirectoryType: directory.Type,
		FS: &FileSystem{
			Path: path.Join(directory.FS.Path, consts.PathData),
		},
	}
	if directory.OSS != nil {
		cfg.OSS = &ObjectStorageService{
			Endpoint:                   directory.OSS.Endpoint,
			Bucket:                     directory.OSS.Bucket,
			AccessKeyID:                directory.OSS.AccessKeyID,
			SecretAccessKey:            directory.OSS.SecretAccessKey,
			CacheDir:                   directory.OSS.CacheDir,
			MinimumConcurrencyLoadSize: directory.OSS.MinimumConcurrencyLoadSize,
		}
	}
	return cfg
}
