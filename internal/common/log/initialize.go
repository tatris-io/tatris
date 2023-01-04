// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package log

import (
	"fmt"
	"sync"

	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/common/log/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var once sync.Once

func SetupLogger(cfg *util.Config) {
	once.Do(func() {
		initLoggers(cfg)
	})
}

func initLoggers(cfg *util.Config) {
	opts := cfg.BuildOpts()

	// init global logger
	if cfg.GlobalLogger == nil {
		return
	}
	coreSlice := make([]zapcore.Core, 0)
	fileConfs := cfg.GlobalLogger.Files
	for _, fileConf := range fileConfs {
		encoder := util.NewTextEncoderByConfig(cfg)
		core, err := util.CreateFileCore(cfg.RootPath, fileConf, cfg.GetLevel(), encoder)
		if err != nil {
			fmt.Printf("init log file errors, file-name: %s, error: %v", fileConf.FileName, err)
			continue
		}
		coreSlice = append(coreSlice, core)
	}

	consoleConfs := cfg.GlobalLogger.Consoles
	for _, consoleConf := range consoleConfs {
		encoder := util.NewTextEncoderByConfig(cfg)
		core := util.CreateConsoleCore(consoleConf, cfg.GetLevel(), encoder)
		coreSlice = append(coreSlice, core)
	}

	globalLoggerCore := zapcore.NewTee(coreSlice...)
	globalLogger := zap.New(globalLoggerCore, opts...)
	logger.SetLogger(globalLogger)
}
