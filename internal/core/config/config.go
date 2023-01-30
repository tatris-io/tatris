// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package config maintains global control parameters for Tatris
package config

import (
	"encoding/json"
	"sync"

	"go.uber.org/atomic"
)

var Cfg *Config

func init() {
	Cfg = &Config{
		Segment: segment{
			MatureThreshold: 500,
		},
	}
}

type Config struct {
	Segment segment

	_once   sync.Once
	_inited atomic.Bool
}

type segment struct {
	MatureThreshold int64 `json:"mature_threshold"`
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

func (s *segment) verify() {
	if s.MatureThreshold <= 0 {
		panic("mature_threshold should be positive")
	}
}

// doVerify verifies the control parameters of all modules
func (cfg *Config) doVerify() {
	cfg.Segment.verify()
}

func (cfg *Config) String() string {
	js, _ := json.Marshal(cfg)
	return string(js)
}
