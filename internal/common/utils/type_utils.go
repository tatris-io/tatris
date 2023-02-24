// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package utils

import (
	"fmt"
	"strconv"
	"time"

	"github.com/tatris-io/tatris/internal/common/errs"

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

func ToFloat64(v interface{}) (float64, error) {
	switch v := v.(type) {
	case float64:
		return v, nil
	case uint64:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, &errs.UnsupportedError{Desc: "can not parse to float64", Value: v}
	}
}

func ToString(v interface{}) string {
	switch v := v.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case uint64:
		return strconv.FormatUint(v, 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int:
		return strconv.FormatInt(int64(v), 10)
	case bool:
		return strconv.FormatBool(v)
	case time.Time:
		return v.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}
