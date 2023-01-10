// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package errors defines error details
package errors

import "fmt"

type Error struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("type: %s, reason: %s", e.Type, e.Reason)
}
