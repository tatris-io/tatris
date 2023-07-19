// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package utils

import (
	"strconv"
	"time"

	"github.com/jinzhu/now"
)

const (
	// 1990-01-01 00:00:00
	unixMilli1990 = 631152000000
)

func ParseTime(value any) (time.Time, error) {
	var t time.Time
	var err error
	switch v := value.(type) {
	case time.Time:
		t = v
	case float64:
		t = UnixToTime(int64(v))
	case int64:
		t = UnixToTime(v)
	case string:
		if unix, err := strconv.ParseInt(v, 10, 64); err == nil {
			t = UnixToTime(unix)
		} else {
			t, err = now.Parse(v)
		}
	}
	return t, err
}

func UnixToTime(n int64) time.Time {
	if n > unixMilli1990*1000000 {
		return time.Unix(0, n)
	}
	if n > unixMilli1990*1000 {
		return time.UnixMicro(n)
	}
	if n > unixMilli1990 {
		return time.UnixMilli(n)
	}
	return time.Unix(n, 0)
}
