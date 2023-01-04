// Copyright 2023 Tatris Project Authors. Licensed under Apache-2.0.

// Package logger holds the mostly used logger instance `GlobalLogger`
package logger

import (
	"github.com/tatris-io/tatris/internal/common/log/util"
	"go.uber.org/zap"
)

var _globalLogger *zap.Logger
var _sugaredGlobalLogger *zap.SugaredLogger

func init() {
	SetLogger(util.GetDefault())
}

// SetLogger inject a logger instance
func SetLogger(instance *zap.Logger) {
	_globalLogger = instance
	_sugaredGlobalLogger = instance.Sugar()
}

// Debug logs a message at DebugLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Debug(msg string, fields ...zap.Field) {
	_globalLogger.Debug(msg, fields...)
}

func Debugf(template string, args ...interface{}) {
	_sugaredGlobalLogger.Debugf(template, args)
}

// Info logs a message at InfoLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Info(msg string, fields ...zap.Field) {
	_globalLogger.Info(msg, fields...)
}

func Infof(template string, args ...interface{}) {
	_sugaredGlobalLogger.Infof(template, args)
}

// Warn logs a message at WarnLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Warn(msg string, fields ...zap.Field) {
	_globalLogger.Warn(msg, fields...)
}

func Warnf(template string, args ...interface{}) {
	_sugaredGlobalLogger.Warnf(template, args)
}

// Error logs a message at ErrorLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
func Error(msg string, fields ...zap.Field) {
	_globalLogger.Error(msg, fields...)
}

func Errorf(template string, args ...interface{}) {
	_sugaredGlobalLogger.Errorf(template, args)
}

// Panic logs a message at PanicLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then panics, even if logging at PanicLevel is disabled.
func Panic(msg string, fields ...zap.Field) {
	_globalLogger.Panic(msg, fields...)
}

func Panicf(template string, args ...interface{}) {
	_sugaredGlobalLogger.Panicf(template, args)
}

// Fatal logs a message at FatalLevel. The message includes any fields passed
// at the log site, as well as any fields accumulated on the logger.
//
// The logger then calls os.Exit(1), even if logging at FatalLevel is
// disabled.
func Fatal(msg string, fields ...zap.Field) {
	_globalLogger.Fatal(msg, fields...)
}
