// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// binary entry point for the standalone mode
package main

import (
	"os"

	"gopkg.in/yaml.v2"

	"github.com/tatris-io/tatris/internal/core/config"

	"github.com/alecthomas/kong"
	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/common/log"
	"github.com/tatris-io/tatris/internal/common/log/logger"
	"github.com/tatris-io/tatris/internal/common/log/util"
	"github.com/tatris-io/tatris/internal/service"
	"go.uber.org/zap"
)

var cli struct {
	Debug bool `help:"Enable debug mode."`
	Conf  struct {
		Logging string `type:"existingfile" help:"Logging config file path."`
		Server  string `type:"existingfile" help:"Server config file path."`
	} `embed:"" prefix:"conf."`
}

// if init failure occurs, the program will use the default logger
func initLoggers(confPath string) {
	content, err := os.ReadFile(confPath)
	if err != nil {
		logger.Panic("fail to init loggers, use the default console logger instead", zap.Error(err))
		return
	}
	var logConf util.Config
	if err := yaml.Unmarshal(content, &logConf); err != nil {
		logger.Panic("fail to init loggers, use the default console logger instead", zap.Error(err))
		return
	}
	// validate all confs
	logConf.Verify()
	log.InitLoggers(&logConf)
	logger.Info("logger initialized successfully", zap.String("config", logConf.String()))
}

func initServer(confPath string) {
	serverConf := config.Cfg
	content, err := os.ReadFile(confPath)
	if err != nil {
		logger.Panic("fail to open server conf, use the default settings instead", zap.Error(err))
		return
	}
	if err := yaml.Unmarshal(content, serverConf); err != nil {
		logger.Panic("fail to init server, use the default settings instead", zap.Error(err))
		return
	}
	// validate all confs
	serverConf.Verify()
	config.Cfg = serverConf
	logger.Info("server initialized successfully", zap.String("config", serverConf.String()))
}

func main() {
	kong.Parse(&cli)

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
	service.StartHTTPServer("all")
}
