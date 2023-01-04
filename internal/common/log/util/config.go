// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

package util

import (
	"fmt"
	"os"
	"path"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	defaultLogMaxSize = 300 // MB
)

// Config contains all log related config
type Config struct {
	// Log level. Only logs with a higher level will be printed
	Level string `json:"level"`
	// Root path for all logs
	RootPath string `json:"root-path"`
	// DisableErrorVerbose stops annotating logs with the full verbose error message.
	DisableErrorVerbose bool `json:"disable-error-verbose"`
	// Development puts the logger in development mode, which changes the
	// behavior of DPanicLevel and takes stacktraces more liberally.
	Development bool `toml:"development" json:"development"`

	// GlobalLogger confs for all common logger
	GlobalLogger *LoggerConfig `json:"global-logger"`

	_level zapcore.Level
}

// validates all input args and call validate func of inner FileConfig and ConsoleConfig
func (cfg Config) validate() {
	finalRootPath, err := calcLogRootPath(cfg.RootPath)
	if err != nil {
		panic(fmt.Errorf("calculate root path errors, mostly because of getting pwd %v", err))
	}
	cfg.RootPath = finalRootPath
	if err := (&cfg._level).UnmarshalText([]byte(cfg.Level)); err != nil {
		panic("invalid level: " + cfg.Level)
	}
	if cfg.GlobalLogger == nil {
		return
	}
	for _, fileConf := range cfg.GlobalLogger.Files {
		fileConf.validate()
	}
	for _, consoleConf := range cfg.GlobalLogger.Consoles {
		consoleConf.validate()
	}
}

// BuildOpts build additional options for zapcore
func (cfg *Config) BuildOpts() []zap.Option {
	opts := make([]zap.Option, 0)
	opts = append(opts, zap.AddStacktrace(zap.ErrorLevel))

	if cfg.Development {
		opts = append(opts, zap.Development())
	}
	return opts
}

// GetLevel gets the overall level for all logging activity
func (cfg *Config) GetLevel() zapcore.Level {
	return cfg._level
}

// LoggerConfig configs a single logger
type LoggerConfig struct {
	// Configs for logs that output to files
	Files []*FileConfig `json:"file-confs"`
	// Configs for logs that output to `stderr` or `stdout`
	Consoles []*ConsoleConfig `json:"console-confs"`
}

// ConsoleConfig configs output to stdout or stderr
type ConsoleConfig struct {
	// ConsoleFD is one of `stdout` or `stderr`
	ConsoleFD string `json:"console-fd"`
	// Highest level you want to output through this console FD
	LevelMax string `json:"level-max"`
	// Lowest level you want to output through this console FD
	LevelMin string `json:"level-min"`

	_minLevel zapcore.Level
	_maxLevel zapcore.Level
}

// Innter validate func for ConsoleConfig. It checks all input config fields and sets default values.
func (cfg *ConsoleConfig) validate() {
	lowercase := strings.ToLower(cfg.ConsoleFD)
	if lowercase != "stderr" && lowercase != "stdout" {
		panic("invalid console-fd: " + cfg.ConsoleFD + ", should be stderr or stdout")
	}
	// zapcore unmarshals the empty string to the `info` level, so we should set defaults in advance
	if len(cfg.LevelMax) == 0 {
		cfg.LevelMax = "fatal"
	}
	if len(cfg.LevelMin) == 0 {
		cfg.LevelMin = "debug"
	}
	if err := (&cfg._minLevel).UnmarshalText([]byte(cfg.LevelMin)); err != nil {
		panic("invalid level-min: " + cfg.LevelMin)
	}
	if err := (&cfg._maxLevel).UnmarshalText([]byte(cfg.LevelMax)); err != nil {
		panic("invalid level-max: " + cfg.LevelMax)
	}
	if cfg._maxLevel < cfg._minLevel {
		panic("invalid logger conf: level-max < level-min")
	}
}

// LogFileConfig configs output to a single log file
type FileConfig struct {
	// Log Filename, default to stdout
	FileName string `json:"file-name"`
	// Max size for a single file, in MB, default is 300MB
	MaxSize int `json:"max-size"`
	// Max days for keeping log files, default is never delete
	MaxDays int `json:"max-days"`
	// Max number of old log files to retain
	MaxBackups int `json:"max-backups"`
	// Highest level you want to output through this file
	LevelMax string `json:"level-max"`
	// Lowest level you want to output through this file
	LevelMin string `json:"level-min"`

	_minLevel zapcore.Level
	_maxLevel zapcore.Level
}

// Innter validate func for FileConfig. It checks all input config fields and sets default values.
func (cfg *FileConfig) validate() {
	if strings.ContainsRune(cfg.FileName, os.PathSeparator) {
		panic("filename should not contain path separator")
	}
	if cfg.MaxBackups < 0 {
		panic("max-backups should be >= 0")
	}
	if cfg.MaxDays < 0 {
		panic("max-days should be >= 0")
	}
	if cfg.MaxSize < 0 {
		panic("max-size should be >= 0")
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = defaultLogMaxSize
	}
	// zapcore unmarshals the empty string to the `info` level, so we should set defaults in advance
	if len(cfg.LevelMax) == 0 {
		cfg.LevelMax = "fatal"
	}
	if len(cfg.LevelMin) == 0 {
		cfg.LevelMin = "debug"
	}
	if err := (&cfg._minLevel).UnmarshalText([]byte(cfg.LevelMin)); err != nil {
		panic("invalid level-min: " + cfg.LevelMin)
	}
	if err := (&cfg._maxLevel).UnmarshalText([]byte(cfg.LevelMax)); err != nil {
		panic("invalid level-max: " + cfg.LevelMax)
	}
	if cfg._maxLevel < cfg._minLevel {
		panic("invalid logger conf: level-max < level-min")
	}
}

// calcLogRootPath calculates the final log dir for all logs
// 1. if nothing is specified, log files will locate at the current working dir.
// 2. if the specified dir is an absolute path, this function does nothing, log files will be printed at the specified dir
// 3. if the specified dir is a relative path, we will join the specified path with current working dir to avoid potential ambiguity
func calcLogRootPath(specified string) (string, error) {
	if len(specified) == 0 {
		return os.Getwd()
	}
	isAbs := path.IsAbs(specified)
	if isAbs {
		return path.Clean(specified), nil
	}
	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return path.Join(pwd, specified), nil
}
