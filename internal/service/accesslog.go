// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package service

import (
	"bytes"
	"time"

	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/common/log/logger"
)

type AccessLogWriter struct {
	gin.ResponseWriter
	body *bytes.Buffer
}

func (w AccessLogWriter) Write(p []byte) (int, error) {
	if n, err := w.body.Write(p); err != nil {
		return n, err
	}
	return w.ResponseWriter.Write(p)
}

func AccessLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		bodyWriter := &AccessLogWriter{body: bytes.NewBufferString(""), ResponseWriter: c.Writer}
		c.Writer = bodyWriter

		start := time.Now().UnixMilli()
		c.Next()
		end := time.Now().UnixMilli()

		logger.Info(
			"access recorded",
			zap.String("remote", c.RemoteIP()),
			zap.String("method", c.Request.Method),
			zap.String("url", c.Request.RequestURI),
			zap.String("proto", c.Request.Proto),
			zap.Int("status", bodyWriter.Status()),
			zap.Int("length", bodyWriter.body.Len()),
			zap.Int64("cost", end-start),
			zap.String("user-agent", c.Request.Header.Get("User-Agent")),
		)
	}
}
