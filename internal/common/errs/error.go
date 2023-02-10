// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package errs defines error details
package errs

import (
	"errors"
	"fmt"
)

var (
	ErrUnsupportedFieldType = errors.New("unsupported field type")
	ErrEmptyMappings        = errors.New("empty mappings")

	ErrNoSegmentMatched   = errors.New("no segment matched")
	ErrIndexLibNotSupport = errors.New("index lib not support")

	ErrSpecifyDirAsFile = errors.New("specify directory as file")
)

type IndexNotFoundError struct {
	Index string `json:"index"`
}

func (e *IndexNotFoundError) Error() string {
	return fmt.Sprintf("index: %s", e.Index)
}

type ShardNotFoundError struct {
	Index string `json:"index"`
	Shard int    `json:"shard"`
}

func (e *ShardNotFoundError) Error() string {
	return fmt.Sprintf("index: %s, shard: %d", e.Index, e.Shard)
}

type SegmentNotFoundError struct {
	Index   string `json:"index"`
	Shard   int    `json:"shard"`
	Segment int    `json:"segment"`
}

func (e *SegmentNotFoundError) Error() string {
	return fmt.Sprintf("index: %s, shard: %d, segment: %d", e.Index, e.Shard, e.Segment)
}

type InvalidFieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e *InvalidFieldError) Error() string {
	return fmt.Sprintf("field: %s, message: %s", e.Field, e.Message)
}

type InvalidValueError struct {
	Field string `json:"field"`
	Value any    `json:"value"`
}

func (e *InvalidValueError) Error() string {
	return fmt.Sprintf("field: %s, value: %v", e.Field, e.Value)
}

type UnsupportedError struct {
	Desc  string `json:"desc"`
	Value any    `json:"value"`
}

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("desc: %s, value: %v", e.Desc, e.Value)
}

type InvalidQueryError struct {
	Query   any    `json:"query"`
	Message string `json:"message"`
}

func (e *InvalidQueryError) Error() string {
	return fmt.Sprintf("query: %v, message: %s", e.Query, e.Message)
}
