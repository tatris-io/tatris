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
			NoSync:           false,
			SegmentSize:      20971520,
			LogFormat:        0,
			SegmentCacheSize: 3,
			NoCopy:           false,
			DirPerms:         0750,
			FilePerms:        0640,
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
	NoSync      bool `yaml:"no_sync"`
	SegmentSize int  `yaml:"segment_size"`
	// 0: Binary; 1: JSON
	LogFormat        byte        `yaml:"log_format"`
	SegmentCacheSize int         `yaml:"segment_cache_size"`
	NoCopy           bool        `yaml:"no_copy"`
	DirPerms         os.FileMode `yaml:"dir_perms"`
	FilePerms        os.FileMode `yaml:"file_perms"`
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
