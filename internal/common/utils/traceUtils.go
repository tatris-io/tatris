// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package utils

import (
	"fmt"
	"time"

	"github.com/tatris-io/tatris/internal/common/log/logger"
)

// Trace is used for logging the execution time of a piece of program
// Usage: defer Trace("do something")()
func Trace(msg string) func() {
	start := time.Now().UnixMilli()
	return func() {
		logger.Infof("%s, cost:%dms", msg, time.Now().UnixMilli()-start)
	}
}

func Tracef(format string, a ...any) func() {
	msg := fmt.Sprintf(format, a...)
	return Trace(msg)
}
