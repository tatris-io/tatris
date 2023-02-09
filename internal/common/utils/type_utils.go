// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package utils

import (
	"time"

	"github.com/jinzhu/now"
)

func IsDateType(value interface{}) bool {
	switch value := value.(type) {
	case string:
		_, err := now.Parse(value)
		return err == nil
	case time.Time:
		return true
	default:
		return false
	}
}

func IsInteger(value interface{}) bool {
	switch value := value.(type) {
	case int, int8, int32, int16, byte:
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
	switch value.(type) {
	case int, int8, int32, int16, byte, float32, float64:
		return true
	default:
		return false
	}
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
