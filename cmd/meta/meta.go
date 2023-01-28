// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// binary entry point for the meta service
package main

import (
	"encoding/json"
	"io/ioutil"

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
		Meta    string `type:"existingfile" help:"Meta config file path."`
	} `embed:"" prefix:"conf."`
}

// if init failure occurs, the program will use the default logger
func initLoggers(confPath string) {
	content, err := ioutil.ReadFile(confPath)
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
}

func main() {
	kong.Parse(&cli)

	if len(cli.Conf.Logging) != 0 {
		initLoggers(cli.Conf.Logging)
	}

	if cli.Debug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}
	service.StartHTTPServer("meta")
}
