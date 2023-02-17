// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package utils

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/tatris-io/tatris/internal/common/log/logger"
)

// Timer is used for logging the execution time of a piece of program
// Usage: defer Timer("do something")()
func Timer(msg string) func() {
	start := time.Now().UnixMilli()
	return func() {
		logger.Info(msg, zap.Int64("cost(ms)", time.Now().UnixMilli()-start))
	}
}

func Timerf(format string, a ...any) func() {
	msg := fmt.Sprintf(format, a...)
	return Timer(msg)
}

func Timestamp2Unix(n int64) time.Time {
	if n > 1e18 {
		return time.Unix(0, n)
	}
	if n > 1e15 {
		return time.UnixMicro(n)
	}
	if n > 1e12 {
		return time.UnixMilli(n)
	}
	return time.Unix(n, 0)
}
