// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package utils

import (
	"regexp"
	"strings"

	"github.com/tatris-io/tatris/internal/common/consts"

	"github.com/minio/pkg/wildcard"
)

const (
	MatchModeRegex    = "regex"
	MatchModeWildcard = "wildcard"
)

func Match(pattern, str, mode string) bool {
	if strings.EqualFold(mode, MatchModeRegex) {
		return RegexMatch(pattern, str)
	}
	return WildcardMatch(pattern, str)
}

func RegexMatch(pattern, str string) bool {
	matched, err := regexp.MatchString(pattern, str)
	return matched && err == nil
}

func WildcardMatch(pattern, str string) bool {
	return wildcard.Match(pattern, str)
}

func ContainsWildcard(str string) bool {
	return strings.Contains(str, consts.Asterisk) || strings.Contains(str, consts.QuestionMark)
}
