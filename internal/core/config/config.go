// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package config maintains global control parameters for Tatris
package config

import (
	"encoding/json"
	"os"
	"sync"

	"go.uber.org/atomic"
)

var Cfg *Config

func init() {
	Cfg = &Config{
		Segment: Segment{
			MatureThreshold: 20000,
		},
		Wal: Wal{
			NoSync:           false,    // Fsync after every write
			SegmentSize:      20971520, // 20 MB log segment files
			LogFormat:        0,        // Binary format is small and fast
			SegmentCacheSize: 3,        // Number of cached in-memory segments
			NoCopy:           false,    // Make a new copy of data for every Read call
			DirPerms:         0750,     // Permissions for the created directories
			FilePerms:        0640,     // Permissions for the created data files
		},
	}
}

type Config struct {
	Segment Segment `yaml:"segment"`
	Wal     Wal     `yaml:"wal"`

	_once   sync.Once
	_inited atomic.Bool
}

type Segment struct {
	MatureThreshold int64 `yaml:"mature_threshold"`
}

type Wal struct {
	// NoSync disables fsync after writes. This is less durable and puts the
	// log at risk of data loss when there's a server crash.
	NoSync bool `yaml:"no_sync"`
	// SegmentSize of each segment. This is just a target value, actual size
	// may differ. Default is 20 MB.
	SegmentSize int `yaml:"segment_size"`
	// LogFormat is the format of the log files. Default is Binary.
	LogFormat byte `yaml:"log_format"`
	// SegmentCacheSize is the maximum number of segments that will be held in
	// memory for caching. Increasing this value may enhance performance for
	// concurrent read operations. Default is 1
	SegmentCacheSize int `yaml:"segment_cache_size"`
	// NoCopy allows for the Read() operation to return the raw underlying data
	// slice. This is an optimization to help minimize allocations. When this
	// option is set, do not modify the returned data because it may affect
	// other Read calls. Default false
	NoCopy bool `yaml:"no_copy"`
	// Perms represents the datafiles modes and permission bits
	DirPerms  os.FileMode `yaml:"dir_perms"`
	FilePerms os.FileMode `yaml:"file_perms"`
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

func (s *Segment) verify() {
	if s.MatureThreshold <= 0 {
		panic("mature_threshold should be positive")
	}
}

func (w *Wal) verify() {
	if w.LogFormat > 1 {
		panic("log_format should be 0 for binary format or 1 for JSON format")
	}
	if w.SegmentSize <= 0 {
		panic("segment_size should be positive")
	}
	if w.SegmentCacheSize <= 0 {
		panic("segment_cache_size should be positive")
	}
}

// doVerify verifies the control parameters of all modules
func (cfg *Config) doVerify() {
	cfg.Segment.verify()
	cfg.Wal.verify()
}

func (cfg *Config) String() string {
	js, _ := json.Marshal(cfg)
	return string(js)
}
