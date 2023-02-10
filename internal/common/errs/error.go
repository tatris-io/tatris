// Copyright 2022 Tatris Project Authors. Licensed under Apache-2.0.

// Package errs defines error details
package errs

import (
	"errors"
	"fmt"
)

var (
	ErrEmptyMappings      = errors.New("empty mappings")
	ErrNoSegmentMatched   = errors.New("no segment matched")
	ErrIndexLibNotSupport = errors.New("index lib not support")
	ErrSpecifyDirAsFile   = errors.New("specify directory as file")
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

type NoShardError struct {
	Index string `json:"index"`
}

func (e *NoShardError) Error() string {
	return fmt.Sprintf("index: %s", e.Index)
}

type SegmentNotFoundError struct {
	Index   string `json:"index"`
	Shard   int    `json:"shard"`
	Segment int    `json:"segment"`
}

func (e *SegmentNotFoundError) Error() string {
	return fmt.Sprintf("index: %s, shard: %d, segment: %d", e.Index, e.Shard, e.Segment)
}

type NoSegmentError struct {
	Index string `json:"index"`
	Shard int    `json:"shard"`
}

func (e *NoSegmentError) Error() string {
	return fmt.Sprintf("index: %s, shard: %d", e.Index, e.Shard)
}

type InvalidFieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

func (e *InvalidFieldError) Error() string {
	return fmt.Sprintf("field: %s, message: %s", e.Field, e.Message)
}

type InvalidFieldValError struct {
	Field string `json:"field"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

func (e *InvalidFieldValError) Error() string {
	return fmt.Sprintf("field: %s, type: %s, value: %v", e.Field, e.Type, e.Value)
}

type UnsupportedError struct {
	Desc  string `json:"desc"`
	Value any    `json:"value"`
}

func (e *UnsupportedError) Error() string {
	return fmt.Sprintf("desc: %s, value: %v", e.Desc, e.Value)
}

type InvalidQueryError struct {
	Message string `json:"message"`
	Query   any    `json:"query"`
}

func (e *InvalidQueryError) Error() string {
	return fmt.Sprintf("message: %s, query: %v", e.Message, e.Query)
}
