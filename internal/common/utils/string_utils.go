// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

package utils

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/tatris-io/tatris/internal/common/errs"

	"github.com/tatris-io/tatris/internal/common/consts"

	"github.com/minio/pkg/wildcard"
)

const (
	MatchModeRegex    = "regex"
	MatchModeWildcard = "wildcard"

	MaxNameBytes = 255
)

var (
	InvalidNameChars       = []rune{'\\', '/', '*', '?', '"', '<', '>', '|', ' ', ',', '#', ':'}
	InvalidNameBorderChars = []rune{'.', '_'}
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

// ValidateResourceName is used to detect whether the naming of various resources such as index,
// alias, and template is legal. This refers to elasticsearch, but it may be stricter than it.
func ValidateResourceName(name string) error {

	if len(name) == 0 {
		return &errs.InvalidResourceNameError{Name: name, Message: fmt.Sprintf("must not be empty")}
	}

	if name != strings.ToLower(name) {
		return &errs.InvalidResourceNameError{Name: name, Message: fmt.Sprintf("must be lowercase")}
	}

	if validateNameContains(name) == false {
		return &errs.InvalidResourceNameError{
			Name: name,
			Message: fmt.Sprintf(
				"must not contain the following characters: %s",
				strings.Join(strings.Split(string(InvalidNameChars), ""), ", "),
			),
		}
	}

	if validateNameBorder(name) == false {
		return &errs.InvalidResourceNameError{
			Name: name,
			Message: fmt.Sprintf(
				"must not start or end with the following characters: %s",
				strings.Join(strings.Split(string(InvalidNameBorderChars), ""), ","),
			),
		}
	}
	runeCount := utf8.RuneCountInString(name)
	if runeCount > MaxNameBytes {
		return &errs.InvalidResourceNameError{
			Name:    name,
			Message: fmt.Sprintf("name is too long, (%d > %d)", runeCount, MaxNameBytes),
		}
	}
	return nil
}

func validateNameContains(name string) bool {
	for _, c := range name {
		for _, invalidChar := range InvalidNameChars {
			if c == invalidChar {
				return false
			}
		}
	}
	return true
}

func validateNameBorder(name string) bool {
	runes := []rune(name)
	for _, invalidBorderChar := range InvalidNameBorderChars {
		if runes[0] == invalidBorderChar || runes[len(runes)-1] == invalidBorderChar {
			return false
		}
	}
	return true
}
