// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package utils

import (
	"time"

	"github.com/jinzhu/now"
)

func IsDateType(value interface{}) bool {
	switch value := value.(type) {
	case time.Time:
		return true
	case string:
		_, err := now.Parse(value)
		return err == nil
	case int64:
		return value > 1e12
	case float64:
		return value > 1e12
	default:
		return false
	}
}

func IsNumeric(value interface{}) bool {
	switch value.(type) {
	case int64, int32, int16, int8, int, byte, float64, float32:
		return true
	default:
		return false
	}
}

func IsInteger(value interface{}) bool {
	switch value := value.(type) {
	case int64, int32, int16, int8, int, byte:
		return true
	case float64:
		return value == float64(int(value))
	case float32:
		return value == float32(int(value))
	default:
		return false
	}
}

func IsFloat(value interface{}) bool {
	return IsNumeric(value)
}

func IsString(value interface{}) bool {
	switch value.(type) {
	case string:
		return true
	default:
		return false
	}
}

func IsBool(value interface{}) bool {
	switch value.(type) {
	case bool:
		return true
	default:
		return false
	}
}
