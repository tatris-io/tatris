// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package config maintains global control parameters for Tatris
package config

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"sync"

	"go.uber.org/atomic"
)

const defaultConfPath = "/conf/server-conf.json"

var Cfg = defaultConfig()

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

// defaultConfig return a default configuration
func defaultConfig() *Config {
	_, filename, _, _ := runtime.Caller(0)
	confFilePath := path.Join(path.Dir(path.Dir(path.Dir(path.Dir(filename)))), defaultConfPath)
	jsonFile, err := os.Open(confFilePath)
	if err != nil {
		panic(fmt.Errorf("open config file from %s failed: %v", confFilePath, err))
	}
	defer jsonFile.Close()
	jsonData, err := io.ReadAll(jsonFile)
	if err != nil {
		panic(fmt.Errorf("read json file failed: %v", err))
	}
	var cfg Config
	err = json.Unmarshal(jsonData, &cfg)
	if err != nil {
		panic(fmt.Errorf("unmarshal json failed: %v", err))
	}
	return &cfg
}

func (cfg *Config) String() string {
	js, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Sprintf("error serializing config to json: %v", err)
	}
	return string(js)
}
