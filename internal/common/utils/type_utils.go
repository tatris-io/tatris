// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package utils

func IsDateType(value interface{}) bool {
	_, err := ParseTime(value)
	return err == nil
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
