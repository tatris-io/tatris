// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateIndexOrAliasName(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		valid bool
	}{
		{
			name:  "valid name",
			input: "my_index123",
			valid: true,
		},
		{
			name:  "valid name with Chinese",
			input: "我的索引",
			valid: true,
		},
		{
			name:  "invalid name with space",
			input: "my index",
			valid: false,
		},
		{
			name:  "invalid name with special character",
			input: "my_index*",
			valid: false,
		},
		{
			name:  "empty name",
			input: "",
			valid: false,
		},
		{
			name:  "invalid name with dot at the beginning",
			input: ".myindex",
			valid: false,
		},
		{
			name:  "invalid name with underscore at the end",
			input: "myindex_",
			valid: false,
		},
		{
			name:  "invalid name with length exceeding limit",
			input: "long_index_name_long_index_name_long_index_name_long_index_name_long_index_name_long_index_name_long_index_name_long_index_name_long_index_name_long_index_name_long_index_name_long_index_name_long_index_name_long_index_name_long_index_name_long_index_name_long_index_name",
			valid: false,
		},
		{
			name:  "invalid Chinese name with length exceeding limit",
			input: "超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称超长的中文名称",
			valid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateResourceName(tc.input)
			assert.Equal(t, tc.valid, err == nil)
		})
	}
}
