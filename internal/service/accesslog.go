// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package service

import (
	"bytes"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tatris-io/tatris/internal/common/log/logger"
)

// [log_time] [log_level] $REMOTE_IP,$METHOD,$URL,$HTTP_PROTO,$STATUS,$BODY_LENGTH,$COST,$USER_AGENT
// e.g. [2023/01/23 14:33:12.603 +08:00] [INFO]
// [127.0.0.1,GET,/v1/search-engine,HTTP/1.1,200,1598,0ms,PostmanRuntime/7.29.2]
const accessLogFmt = "%s,%s,%s,%s,%d,%d,%dms,%s"

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

		logger.Infof(
			accessLogFmt,
			c.RemoteIP(),
			c.Request.Method,
			c.Request.RequestURI,
			c.Request.Proto,
			bodyWriter.Status(),
			bodyWriter.body.Len(),
			end-start,
			c.Request.Header.Get("User-Agent"),
		)
	}
}
