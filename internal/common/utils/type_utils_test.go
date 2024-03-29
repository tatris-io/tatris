// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDateParse(t *testing.T) {
	dates := []string{
		"2023-01-26T00:00:40Z",
		"2023-01-26 08:00:40",

		"2023-01-26 08:00:40.000",
		"2023-01-26T00:00:40.000Z",
	}
	for _, date := range dates {
		t.Log(date)
		assert.True(t, IsDateType(date))
	}
}
