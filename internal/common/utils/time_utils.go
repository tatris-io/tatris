// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package utils

import (
	"time"

	"github.com/jinzhu/now"
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
		t, err = now.Parse(v)
	}
	return t, err
}

func UnixToTime(n int64) time.Time {
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
