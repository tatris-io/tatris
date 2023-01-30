// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package cmd describes how to start Tatris
package cmd

import (
	"encoding/json"
	"os"

	"github.com/tatris-io/tatris/internal/core/config"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/common/log"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/common/log/util"
	"go.uber.org/zap"
)

type Cli struct {
	Debug bool `help:"Enable debug mode."`
	Conf  struct {
		Logging string `type:"existingfile" help:"Logging config file path."`
		Server  string `type:"existingfile" help:"Server config file path."`
	} `                          embed:"" prefix:"conf."`
}

// Initialize tatris through the command-line
func Initialize(cli *Cli) {
	if len(cli.Conf.Logging) != 0 {
		initLoggers(cli.Conf.Logging)
	}
	if len(cli.Conf.Server) != 0 {
		initServer(cli.Conf.Server)
	}
	if cli.Debug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
}

// init loggers
// if init failure occurs, the program will use the default logger
func initLoggers(confPath string) {
	logger.Info("logger is initializing")
	content, err := os.ReadFile(confPath)
	if err != nil {
		logger.Panic("fail to init loggers, use the default console logger instead", zap.Error(err))
		return
	}
	var logConf util.Config
	if err := json.Unmarshal(content, &logConf); err != nil {
		logger.Panic("fail to init loggers, use the default console logger instead", zap.Error(err))
		return
	}
	// validate all confs
	logConf.Verify()
	log.InitLoggers(&logConf)
	logger.Info("logger initialized successfully", zap.String("config", logConf.String()))
}

// init tatris server
// if init failure occurs, the program will use conf/server-conf.json as default settings
func initServer(confPath string) {
	content, err := os.ReadFile(confPath)
	if err != nil {
		logger.Panic("fail to open server conf, use the default settings instead", zap.Error(err))
		return
	}
	serverConf := config.Cfg
	if err := json.Unmarshal(content, serverConf); err != nil {
		logger.Panic("fail to init server, use the default settings instead", zap.Error(err))
		return
	}
	// validate all confs
	serverConf.Verify()
	logger.Info("server initialized successfully", zap.String("config", config.Cfg.String()))
}
