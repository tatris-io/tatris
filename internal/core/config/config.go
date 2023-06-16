// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package config maintains global control parameters for Tatris
package config

import (
	"encoding/json"
	"os"
	"sync"

	"github.com/tatris-io/tatris/internal/common/consts"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"go.uber.org/zap"

	"go.uber.org/atomic"
)

var Cfg *Config

func init() {
	Cfg = &Config{
		IndexLib: consts.IndexLibBluge,
		Directory: &Directory{
			Type: consts.DirectoryFS,
			FS: &FS{
				Path: consts.DefaultFSPath,
			},
		},
		Segment: &Segment{
			MatureThreshold: 20000,
		},
		Wal: &Wal{
			NoSync:           false,
			SegmentSize:      20971520,
			LogFormat:        0,
			SegmentCacheSize: 3,
			NoCopy:           false,
			DirPerms:         0750,
			FilePerms:        0640,
			Parallel:         16,
		},
		Query: &Query{
			Parallel:                    16,
			DefaultScanHours:            24 * 3,
			DefaultAggregationShardSize: 5000,
			MaxDocNum:                   1000000,
		},
	}
}

type Config struct {
	IndexLib  string     `yaml:"index_lib"`
	Directory *Directory `yaml:"directory"`
	Segment   *Segment   `yaml:"segment"`
	Wal       *Wal       `yaml:"wal"`
	Query     *Query     `yaml:"query"`

	_once   sync.Once
	_inited atomic.Bool
}

type Directory struct {
	Type string `yaml:"type"`
	FS   *FS    `yaml:"fs"`
	OSS  *OSS   `yaml:"oss"`
}

type FS struct {
	Path string `yaml:"path"`
}

type OSS struct {
	Endpoint        string `yaml:"endpoint"`
	Bucket          string `yaml:"bucket"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	// MinimumConcurrencyLoadSize is the minimum file size to enable concurrent query.
	// When the file size to be loaded is greater than this value, oss will be queried concurrently
	MinimumConcurrencyLoadSize int `yaml:"minimum_concurrency_load_size"`
}

type Segment struct {
	MatureThreshold int64 `yaml:"mature_threshold"`
}

type Wal struct {
	NoSync      bool `yaml:"no_sync"`
	SegmentSize int  `yaml:"segment_size"`
	// 0: Binary; 1: JSON
	LogFormat        byte        `yaml:"log_format"`
	SegmentCacheSize int         `yaml:"segment_cache_size"`
	NoCopy           bool        `yaml:"no_copy"`
	DirPerms         os.FileMode `yaml:"dir_perms"`
	FilePerms        os.FileMode `yaml:"file_perms"`
	// the number of Goroutines used to consume WAL each time
	Parallel int `yaml:"parallel"`
}

type Query struct {
	// the number of Goroutines used to retrieve multiple segments per query
	Parallel int `yaml:"parallel"`
	// the default number of hours to scan when no time range is explicitly passed in
	DefaultScanHours int `yaml:"default_scan_hours"`
	// acts on terms aggregation, increase default_aggregation_shard_size
	// to better account for these disparate doc counts and improve the accuracy
	// of the selection of top terms
	DefaultAggregationShardSize int `yaml:"default_aggregation_shard_size"`
	// The maximum number of documents that a query is allowed to hit, an errs.QueryLoadExceedError
	// will be returned if this limit is exceeded.
	MaxDocNum int64 `yaml:"max_doc_num"`
}

// Verify wraps doVerify with a `sync.Once`
func (cfg *Config) Verify() {
	cfg._once.Do(func() {
		cfg.doVerify()
		cfg._inited.Store(true)
	})
}

// IsVerified checks if this config struct was verified or not
func (cfg *Config) IsVerified() bool {
	return cfg._inited.Load()
}

func (dir *Directory) verify() {
	dir.FS.verify()
	if dir.Type == consts.DirectoryOSS {
		dir.OSS.verify()
	}
}

func (fs *FS) verify() {
	if stat, err := os.Stat(fs.Path); err != nil {
		if os.IsNotExist(err) {
			// not exists, try to create
			if err = os.MkdirAll(fs.Path, 0755); err != nil {
				logger.Panic("create data path failed", zap.String("path", fs.Path), zap.Error(err))
			}
			logger.Info("create data path", zap.String("path", fs.Path))
		} else {
			logger.Panic("invalid data path", zap.String("path", fs.Path), zap.Error(err))
		}
	} else {
		// already exists, check if it is a DIRECTORY
		if !stat.IsDir() {
			logger.Panic("data path is not a directory", zap.String("path", fs.Path))
		}
	}
}

func (oss *OSS) verify() {
	if oss.Endpoint == "" || oss.Bucket == "" || oss.AccessKeyID == "" ||
		oss.SecretAccessKey == "" {
		logger.Panic(
			"endpoint, bucket, access_key_id, secret_access_key must be specified when directory type is oss",
		)
	}
}

func (s *Segment) verify() {
	if s.MatureThreshold <= 0 {
		panic("segment.mature_threshold should be positive")
	}
}

func (w *Wal) verify() {
	if w.LogFormat > 1 {
		panic("wal.log_format should be 0 for binary format or 1 for JSON format")
	}
	if w.SegmentSize <= 0 {
		panic("wal.segment_size should be positive")
	}
	if w.SegmentCacheSize <= 0 {
		panic("wal.segment_cache_size should be positive")
	}
	if w.Parallel <= 0 {
		panic("wal.parallel should be positive")
	}
}

func (q *Query) verify() {
	if q.Parallel <= 0 {
		panic("query.parallel should be positive")
	}
}

// doVerify verifies the control parameters of all modules
func (cfg *Config) doVerify() {
	cfg.Directory.verify()
	cfg.Segment.verify()
	cfg.Wal.verify()
	cfg.Query.verify()
}

func (cfg *Config) GetFSPath() string {
	return cfg.Directory.FS.Path
}

func (cfg *Config) String() string {
	js, _ := json.Marshal(cfg)
	return string(js)
}
